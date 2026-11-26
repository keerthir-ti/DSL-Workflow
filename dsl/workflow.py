"""GraphInterpreterWorkflow: the deterministic DSL interpreter.

Rules enforced here (see SOLUTION_DESIGN.md §5.2):
- the graph reaches this code only as recorded activity results (fetch_next_node);
- decisions use only input, activity/child/timer results, signals/updates and workflow.* APIs;
- a fixed set of node types; business kinds run inside the execute_node activity;
- continue-as-new only at a node boundary, after fetch_next_node has committed the position.

`Interpreter` holds the loop and handlers. Each step of the loop is a small overridable method so
test variants can change exactly one decision and show its effect on replay.
"""

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import Any, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from dsl.models import (
        CONFIG_ERROR,
        WORKFLOW_NAME,
        Event,
        ExecuteInput,
        FetchInput,
        GotoInput,
        GotoMode,
        GotoRequest,
        JourneyInput,
        NodeSpec,
        NodeStatus,
        ParticipantInput,
        Position,
    )

INTERRUPTED = "__interrupted__"  # a wait was interrupted by goto
CHUNK = "__chunk__"  # a timer chunk elapsed before the deadline
RECHECK = "__recheck__"  # a wait was nudged: re-fetch the current node from the latest graph

# Transient failures retry without limit; business errors (ConfigError) are non-retryable and park the journey.
ACTIVITY_OPTIONS: Dict[str, Any] = dict(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=200), maximum_interval=timedelta(seconds=5)),
)


def is_config_error(err: BaseException) -> bool:
    cause = getattr(err, "cause", None)
    return isinstance(cause, ApplicationError) and cause.type == CONFIG_ERROR


class Interpreter:
    def __init__(self) -> None:
        self.inp: Optional[JourneyInput] = None
        self.pos = Position()
        self.status = "RUNNING"
        self.hops = 0
        self.events: Dict[str, List[Any]] = {}
        self.goto_target: Optional[str] = None
        self.goto_mode = GotoMode.IN_PLACE
        self.resume_requested = False
        self.chunk_elapsed = False
        self.nudged = False
        self.current_node: Optional[str] = None

    # ---------------------------------------------------------------- loop
    async def interpret(self, inp: JourneyInput) -> Position:
        self.inp = inp
        for name, payloads in inp.pending_events.items():
            self.events.setdefault(name, []).extend(payloads)
        self.pos = await self.activity("load_position", inp.journey_id, Position)
        start_node = inp.start_node
        while True:
            goto = start_node or await self.apply_goto()
            start_node = None
            await self.before_fetch()
            spec = await self.fetch(self.pos, goto)
            if spec.status == NodeStatus.END:
                self.status = "COMPLETED"
                return self.pos
            if self.continue_as_new_due():
                await self.continue_as_new(start_node=goto)
            if spec.status == NodeStatus.ORPHANED:
                await self.park()
                continue
            step_no = self.pos.step_no + 1
            self.current_node = spec.node_id
            try:
                outcome = await self.run_node(spec, step_no)
            except ActivityError as err:
                if not is_config_error(err):
                    raise
                await self.on_config_error(err)
                continue
            if outcome == INTERRUPTED:
                continue
            if outcome in (CHUNK, RECHECK):
                wait_started_at = self.pos.wait_started_at if spec.type == "timer" else None
                self.pos = Position(spec.node_id, self.pos.step_no, None, spec.graph_version, wait_started_at)
                self.chunk_elapsed = outcome == CHUNK
                continue
            self.pos = Position(spec.node_id, step_no, outcome, spec.graph_version)
            self.hops += 1

    # ------------------------------------------------------- overridable steps
    async def before_fetch(self) -> None:
        pass

    async def on_config_error(self, err: ActivityError) -> None:
        await self.park()

    async def fetch(self, pos: Position, goto: Optional[str]) -> NodeSpec:
        return await self.activity(
            "fetch_next_node", FetchInput(self.inp.journey_id, self.inp.graph_id, pos, goto), NodeSpec
        )

    async def run_node(self, spec: NodeSpec, step_no: int) -> str:
        if spec.type == "activity":
            return await self.run_activity_node(spec, step_no)
        if spec.type == "timer":
            return await self.run_timer_node(spec)
        if spec.type == "signal":
            return await self.run_signal_node(spec)
        if spec.type == "fanout":
            return await self.run_fanout_node(spec)
        raise ApplicationError(f"unknown node type {spec.type}", non_retryable=True)

    async def run_activity_node(self, spec: NodeSpec, step_no: int) -> str:
        return await self.activity(
            "execute_node",
            ExecuteInput(self.inp.journey_id, self.inp.graph_id, step_no, spec.node_id, spec.graph_version),
            str,
        )

    async def run_timer_node(self, spec: NodeSpec) -> str:
        now = workflow.now().timestamp()
        resuming = self.pos.node_id == spec.node_id and self.pos.last_outcome is None and self.pos.wait_started_at
        started = self.pos.wait_started_at if resuming else now
        remaining = started + float(spec.config.get("seconds", 0)) - now
        if remaining <= 0:
            return "fired"
        chunk = self.inp.timer_chunk_seconds
        wait = min(remaining, chunk) if chunk else remaining
        self.pos.wait_started_at = started
        try:
            await workflow.wait_condition(self.interrupted, timeout=timedelta(seconds=wait))
            return self.interruption()
        except asyncio.TimeoutError:
            return CHUNK if wait < remaining else "fired"

    async def run_signal_node(self, spec: NodeSpec) -> str:
        name = spec.config["name"]
        await workflow.wait_condition(lambda: bool(self.events.get(name)) or self.interrupted())
        if self.interrupted():
            return self.interruption()
        self.events[name].pop(0)
        return "received"

    def interrupted(self) -> bool:
        return self.goto_target is not None or self.nudged

    def interruption(self) -> str:
        if self.goto_target is not None:
            return INTERRUPTED
        self.nudged = False
        return RECHECK

    async def run_fanout_node(self, spec: NodeSpec) -> str:
        graph_id = spec.config["graph_id"]
        for participant in spec.config.get("participants", []):
            child_id = f"{self.inp.journey_id}:{participant}"
            if spec.config.get("via") == "activity":
                await self.activity("start_participant", ParticipantInput(self.inp.journey_id, participant, graph_id), str)
            else:
                await workflow.start_child_workflow(
                    WORKFLOW_NAME,
                    JourneyInput(journey_id=child_id, graph_id=graph_id),
                    id=child_id,
                    parent_close_policy=workflow.ParentClosePolicy[spec.config.get("close_policy", "ABANDON")],
                )
        return "ok"

    # ------------------------------------------------------------- helpers
    async def activity(self, name: str, arg: Any, result_type: Any) -> Any:
        return await workflow.execute_activity(name, arg, result_type=result_type, **ACTIVITY_OPTIONS)

    def continue_as_new_due(self) -> bool:
        if self.chunk_elapsed:
            return True
        if self.inp.hops_per_run and self.hops >= self.inp.hops_per_run:
            return True
        return workflow.info().is_continue_as_new_suggested()

    async def continue_as_new(self, start_node: Optional[str]) -> None:
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.continue_as_new(replace(
            self.inp,
            start_node=start_node if start_node is not None else self.goto_target,
            pending_events={name: payloads for name, payloads in self.events.items() if payloads},
        ))

    async def park(self) -> None:
        self.status = "NEEDS_ATTENTION"
        await workflow.wait_condition(lambda: self.resume_requested or self.goto_target is not None)
        self.resume_requested = False
        self.status = "RUNNING"

    async def apply_goto(self) -> Optional[str]:
        """Consume a pending goto at a node boundary: in place, or as a continue-as-new with that start node."""
        if self.goto_target is None:
            return None
        target, mode = self.goto_target, self.goto_mode
        self.goto_target, self.goto_mode = None, GotoMode.IN_PLACE
        if mode == GotoMode.CONTINUE_AS_NEW:
            await self.continue_as_new(start_node=target)
        return target

    # ------------------------------------------------------------- handlers
    @workflow.signal
    def event(self, e: Event) -> None:
        self.events.setdefault(e.name, []).append(e.payload)

    @workflow.signal
    def nudge(self) -> None:
        """'Look again': a waiting node re-reads its latest definition (e.g. after a publish)."""
        self.nudged = True

    @workflow.signal
    def goto_signal(self, target: str) -> None:
        self.goto_target = target

    @workflow.update
    async def goto(self, req: GotoRequest) -> str:
        ok = await self.activity("validate_goto", GotoInput(self.inp.journey_id, self.inp.graph_id, req.target), bool)
        if not ok:
            return "rejected"
        self.goto_target, self.goto_mode = req.target, req.mode
        return "accepted"

    @workflow.update
    def resume(self) -> str:
        self.resume_requested = True
        return "resumed"

    @workflow.query
    def state(self) -> Dict[str, Any]:
        return {"status": self.status, "current_node": self.current_node, "last_node": self.pos.node_id,
                "step_no": self.pos.step_no, "graph_version": self.pos.graph_version, "hops_this_run": self.hops}


@workflow.defn(name=WORKFLOW_NAME)
class GraphInterpreterWorkflow(Interpreter):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)
