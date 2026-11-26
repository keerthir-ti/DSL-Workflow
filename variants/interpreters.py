"""Interpreter variants. Each overrides exactly one decision of the safe interpreter (dsl/workflow.py)."""

from datetime import timedelta
from typing import Any, Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from dsl.models import Event, ExecuteInput, FetchInput, GotoRequest, JourneyInput, NodeSpec, Position
    from dsl.resolve import resolve_next
    from dsl.workflow import WORKFLOW_NAME, Interpreter
    from variants import shared_state


class _GraphInWorkflow(Interpreter):
    """Base for variants that decide the next node inside workflow code instead of in an activity."""

    def __init__(self) -> None:
        super().__init__()
        self.done: Dict[str, str] = {}

    def graph(self) -> Dict[str, Any]:
        raise NotImplementedError

    async def load_graph(self) -> Dict[str, Any]:
        return self.graph()

    async def fetch(self, pos: Position, goto: Optional[str]) -> NodeSpec:
        if pos.node_id and pos.last_outcome:
            self.done[pos.node_id] = pos.last_outcome
        return resolve_next(await self.load_graph(), None, pos, self.done, goto)


# ---------------------------------------------------------------- Q3: graph read in workflow code
@workflow.defn(name=WORKFLOW_NAME)
class GraphInGlobal(_GraphInWorkflow):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    def graph(self):
        return shared_state.GRAPH["latest"]


@workflow.defn(name=WORKFLOW_NAME)
class GraphInProcessCache(_GraphInWorkflow):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def load_graph(self):
        graph_id = self.inp.graph_id
        if graph_id not in shared_state.CACHE:
            shared_state.CACHE[graph_id] = await self.activity("get_graph", graph_id, dict)
        return shared_state.CACHE[graph_id]


# ------------------------------------------------- Q3: node type -> command mapping changed in code
@workflow.defn(name=WORKFLOW_NAME)
class TimerAsActivity(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_timer_node(self, spec):
        return await self.activity("durable_wait", float(spec.config.get("seconds", 0)), str)


@workflow.defn(name=WORKFLOW_NAME)
class TimerAsActivityPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_timer_node(self, spec):
        if workflow.patched("timer-as-activity"):
            return await self.activity("durable_wait", float(spec.config.get("seconds", 0)), str)
        return await super().run_timer_node(spec)


@workflow.defn(name=WORKFLOW_NAME)
class ActivityRenamed(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        inp = ExecuteInput(self.inp.journey_id, self.inp.graph_id, step_no, spec.node_id, spec.graph_version)
        return await self.activity("run_node", inp, str)


@workflow.defn(name=WORKFLOW_NAME)
class FanoutViaActivity(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_fanout_node(self, spec):
        spec.config = {**spec.config, "via": "activity"}
        return await super().run_fanout_node(spec)


# --------------------------------------- Q3/Q4/Q9: interpreter loop change (extra activity per hop)
@workflow.defn(name=WORKFLOW_NAME)
class ExtraActivityUnpatched(Interpreter):
    """v2 unpatched; identical code is also 'v4: patch removed'."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        await self.activity("audit", self.inp.journey_id, type(None))


@workflow.defn(name=WORKFLOW_NAME)
class ExtraActivityPatched(Interpreter):
    """v2: patch."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        if workflow.patched("audit-before-fetch"):
            await self.activity("audit", self.inp.journey_id, type(None))


@workflow.defn(name=WORKFLOW_NAME)
class ExtraActivityDeprecated(Interpreter):
    """v3: deprecate_patch."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        workflow.deprecate_patch("audit-before-fetch")
        await self.activity("audit", self.inp.journey_id, type(None))


# -------------------------------------------------------- Q3: per-client flag read in workflow code
@workflow.defn(name=WORKFLOW_NAME)
class ClientFlagInCode(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        if shared_state.CLIENT_FLAG["extra_step"]:
            await self.activity("audit", self.inp.journey_id, type(None))


# ------------------------------------------------------------- Q3: workflow.uuid4() call order
@workflow.defn(name=WORKFLOW_NAME)
class UuidOneCall(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        shared_state.SEEN_IDS.append(str(workflow.uuid4()))
        return await super().run_activity_node(spec, step_no)


@workflow.defn(name=WORKFLOW_NAME)
class UuidTwoCalls(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        workflow.uuid4()  # a new, unrelated uuid4() call added before the existing one
        shared_state.SEEN_IDS.append(str(workflow.uuid4()))
        return await super().run_activity_node(spec, step_no)


# ------------------------------------------------- Q3 control group: changes Temporal does not check
@workflow.defn(name=WORKFLOW_NAME)
class ControlActivityOptions(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def activity(self, name, arg, result_type):
        return await workflow.execute_activity(
            name, arg, result_type=result_type, start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


@workflow.defn(name=WORKFLOW_NAME)
class ControlActivityArgs(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        inp = ExecuteInput(self.inp.journey_id, self.inp.graph_id, step_no, spec.node_id, None)
        return await self.activity("execute_node", inp, str)


@workflow.defn(name=WORKFLOW_NAME)
class ControlTimerDuration(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_timer_node(self, spec):
        spec.config = {**spec.config, "seconds": float(spec.config.get("seconds", 0)) * 2}
        return await super().run_timer_node(spec)


# ------------------------------------------- Q5: remove an activity from the loop (merge for cost)
@workflow.defn(name=WORKFLOW_NAME)
class MergedFetchExecuteUnpatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def fetch(self, pos, goto):
        inp = FetchInput(self.inp.journey_id, self.inp.graph_id, pos, goto)
        return await self.activity("fetch_and_execute", inp, NodeSpec)

    async def run_activity_node(self, spec, step_no):
        return spec.config["__outcome"]


@workflow.defn(name=WORKFLOW_NAME)
class MergedFetchExecutePatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def fetch(self, pos, goto):
        if workflow.patched("merge-fetch-execute"):
            inp = FetchInput(self.inp.journey_id, self.inp.graph_id, pos, goto)
            return await self.activity("fetch_and_execute", inp, NodeSpec)
        return await super().fetch(pos, goto)

    async def run_activity_node(self, spec, step_no):
        if workflow.patched("merge-fetch-execute"):
            return spec.config["__outcome"]
        return await super().run_activity_node(spec, step_no)


# ------------------------------------------------------------------------------- Q6: recovery
@workflow.defn(name=WORKFLOW_NAME)
class ConfigAsActivityInput(Interpreter):
    """Anti-pattern: node config passed as activity input; retries reuse the original input."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        return await workflow.execute_activity(
            "execute_node_with_config", spec, result_type=str, start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=200), backoff_coefficient=1.0),
        )


@workflow.defn(name=WORKFLOW_NAME)
class FailOnConfigError(Interpreter):
    """No parking: a config error fails the workflow (closed), so only a reset can resume it."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def on_config_error(self, err: ActivityError):
        raise ApplicationError(f"node failed: {err.cause}", non_retryable=True)


# ------------------------------------------------- more code changes (tests/test_more_nde_scenarios.py)
NODE_ATTRIBUTE = SearchAttributeKey.for_keyword("DslNode")


@workflow.defn(name=WORKFLOW_NAME)
class ContinueAsNewEveryHop(Interpreter):
    """The continue-as-new condition is changed in code: now after every node."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    def continue_as_new_due(self):
        return self.hops >= 1 or super().continue_as_new_due()


@workflow.defn(name=WORKFLOW_NAME)
class GotoWithoutValidation(Interpreter):
    """The goto update handler no longer calls the validate_goto activity."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    @workflow.update
    async def goto(self, req: GotoRequest) -> str:
        self.goto_target, self.goto_mode = req.target, req.mode
        return "accepted"


@workflow.defn(name=WORKFLOW_NAME)
class EventHandlerCountsEvents(Interpreter):
    """Control: the signal handler changes but issues no commands."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    @workflow.signal
    def event(self, e: Event) -> None:
        self.events.setdefault(e.name, []).append(e.payload)
        self.events_seen = getattr(self, "events_seen", 0) + 1


@workflow.defn(name=WORKFLOW_NAME)
class FanoutReversedOrder(Interpreter):
    """Participants are started in the opposite order."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_fanout_node(self, spec):
        spec.config = {**spec.config, "participants": list(reversed(spec.config.get("participants", [])))}
        return await super().run_fanout_node(spec)


@workflow.defn(name=WORKFLOW_NAME)
class UpsertNodeAttribute(Interpreter):
    """A search attribute upsert is added before every fetch."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        workflow.upsert_search_attributes([NODE_ATTRIBUTE.value_set(self.pos.node_id or "start")])


# --------------------------------------------- fixes: the same changes behind workflow.patched()
@workflow.defn(name=WORKFLOW_NAME)
class ActivityRenamedPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_activity_node(self, spec, step_no):
        if workflow.patched("rename-execute-node"):
            inp = ExecuteInput(self.inp.journey_id, self.inp.graph_id, step_no, spec.node_id, spec.graph_version)
            return await self.activity("run_node", inp, str)
        return await super().run_activity_node(spec, step_no)


@workflow.defn(name=WORKFLOW_NAME)
class FanoutViaActivityPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_fanout_node(self, spec):
        if workflow.patched("fanout-via-activity"):
            spec.config = {**spec.config, "via": "activity"}
        return await super().run_fanout_node(spec)


@workflow.defn(name=WORKFLOW_NAME)
class ContinueAsNewEveryHopPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    def continue_as_new_due(self):
        if workflow.patched("continue-as-new-every-hop") and self.hops >= 1:
            return True
        return super().continue_as_new_due()


@workflow.defn(name=WORKFLOW_NAME)
class GotoWithoutValidationPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    @workflow.update
    async def goto(self, req: GotoRequest) -> str:
        if not workflow.patched("goto-without-validation"):
            return await super().goto(req)
        self.goto_target, self.goto_mode = req.target, req.mode
        return "accepted"


@workflow.defn(name=WORKFLOW_NAME)
class FanoutReversedOrderPatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def run_fanout_node(self, spec):
        if workflow.patched("fanout-reversed-order"):
            spec.config = {**spec.config, "participants": list(reversed(spec.config.get("participants", [])))}
        return await super().run_fanout_node(spec)


@workflow.defn(name=WORKFLOW_NAME)
class UpsertNodeAttributePatched(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        if workflow.patched("upsert-node-attribute"):
            workflow.upsert_search_attributes([NODE_ATTRIBUTE.value_set(self.pos.node_id or "start")])


@workflow.defn(name=WORKFLOW_NAME)
class ClientFlagFromActivity(Interpreter):
    """Fix for the per-client flag: read it through an activity, so the value is recorded in history."""

    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    async def before_fetch(self):
        if await self.activity("get_client_flag", self.inp.graph_id, bool):
            await self.activity("audit", self.inp.journey_id, type(None))
