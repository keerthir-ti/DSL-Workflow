"""Shared test helpers: graph builders, worker/context factory, history capture and replay."""

import asyncio
import json
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHandle, WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from dsl.activities import DslActivities
from reference.handlers import ExampleHandlers
from reference.json_store import JsonGraphStore, JsonLedger
from dsl.models import Event, JourneyInput
from dsl.workflow import WORKFLOW_NAME, GraphInterpreterWorkflow
from tests.probe import ReplayProbe
from variants.activities import VariantActivities

HISTORIES = Path(__file__).resolve().parent.parent / "histories"

# The unsafe variants read process-wide state. Like a real worker that marks its config/cache module as
# pass-through, the sandbox must share these modules with the host process (otherwise each run gets a copy).
RUNNER = SandboxedWorkflowRunner(
    restrictions=SandboxRestrictions.default.with_passthrough_modules("variants.shared_state", "variants.graph_file_loader")
)


# ------------------------------------------------------------------ graph builders
def graph(start: str, graph_id: str = "g", **nodes: Dict[str, Any]) -> Dict[str, Any]:
    return {"graph_id": graph_id, "start": start, "nodes": nodes}


def act(next: Optional[str] = None, kind: str = "noop", **config: Any) -> Dict[str, Any]:
    return {"type": "activity", "kind": kind, "config": config, "next": {"ok": next}}


def sig(name: str, next: Optional[str] = None) -> Dict[str, Any]:
    return {"type": "signal", "config": {"name": name}, "next": {"received": next}}


def timer(seconds: float, next: Optional[str] = None) -> Dict[str, Any]:
    return {"type": "timer", "config": {"seconds": seconds}, "next": {"fired": next}}


def fanout(participants: List[str], graph_id: str, next: Optional[str] = None, **config: Any) -> Dict[str, Any]:
    return {"type": "fanout", "config": {"participants": participants, "graph_id": graph_id, **config}, "next": {"ok": next}}


# ------------------------------------------------------------------ test context
@dataclass
class Ctx:
    env: WorkflowEnvironment
    root: Path
    prefix: str = field(default_factory=lambda: uuid.uuid4().hex[:8])

    def __post_init__(self) -> None:
        self.store = JsonGraphStore(self.root)
        self.ledger = JsonLedger(self.root)
        self.handlers = ExampleHandlers()
        self.task_queue = f"tq-{self.prefix}"
        self.dsl = DslActivities(self.store, self.ledger, self.handlers.as_dict(), self.env.client, self.task_queue)
        self.extra = VariantActivities(self.dsl)

    @property
    def client(self):
        return self.env.client

    def jid(self, name: str) -> str:
        return f"{self.prefix}-{name}"

    def worker(self, *workflows: Type, activities: Optional[List[Callable]] = None, **kwargs: Any) -> Worker:
        if self.env.supports_time_skipping:
            # The time-skipping test server never moves a task off a stopped worker's sticky queue (a real
            # server does after the sticky timeout), so test-server workers run without a cache.
            kwargs.setdefault("max_cached_workflows", 0)
        return Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=list(workflows) or [GraphInterpreterWorkflow],
            activities=activities if activities is not None else self.dsl.all() + self.extra.all(),
            interceptors=[ReplayProbe()],
            workflow_runner=RUNNER,
            **kwargs,
        )

    async def start(self, name: str, graph_id: str = "g", **kwargs: Any) -> WorkflowHandle:
        journey_id = self.jid(name)
        return await self.client.start_workflow(
            WORKFLOW_NAME, JourneyInput(journey_id=journey_id, graph_id=graph_id, **kwargs),
            id=journey_id, task_queue=self.task_queue,
        )

    def handle(self, name: str) -> WorkflowHandle:
        return self.client.get_workflow_handle(self.jid(name))

    def effects(self, name: str) -> List[str]:
        """Node ids of completed steps, in order (from the ledger)."""
        return [s["node_id"] for s in self.ledger.steps(self.jid(name))]


async def send(handle: WorkflowHandle, name: str, payload: Any = None) -> None:
    await handle.signal("event", Event(name, payload))


async def wait_until(predicate: Callable[[], Any], timeout: float = 15.0, interval: float = 0.1) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("condition not met in time")
        await asyncio.sleep(interval)


async def park(
    ctx: Ctx, name: str, at: str = "n2", workflow_cls: Type = GraphInterpreterWorkflow, graph: Optional[Dict[str, Any]] = None,
) -> Tuple[WorkflowHandle, WorkflowHistory]:
    """Start a journey on workflow_cls, wait until it stands on node `at`, return its handle and history."""
    if graph is not None:
        ctx.store.publish(graph)
    async with ctx.worker(workflow_cls):
        handle = await ctx.start(name)
        await wait_at(handle, at)
        return handle, await handle.fetch_history()


async def terminate(ctx: Ctx, *workflow_ids: str) -> None:
    for workflow_id in workflow_ids:
        await ctx.client.get_workflow_handle(workflow_id).terminate()


async def wait_at(handle: WorkflowHandle, node_id: str, status: str = "RUNNING", timeout: float = 15.0) -> Dict[str, Any]:
    """Wait until the journey is standing on node_id (e.g. parked on a signal or timer)."""

    async def check():
        try:
            state = await handle.query("state")
        except Exception:
            return None
        return state if state["current_node"] == node_id and state["status"] == status else None

    return await wait_until(check, timeout)


# ------------------------------------------------------------------ history and replay
def event_types(history: WorkflowHistory) -> List[str]:
    return [EventType.Name(e.event_type).replace("EVENT_TYPE_", "") for e in history.events]


def workflow_task_failures(history: WorkflowHistory) -> List[str]:
    """Failure messages of failed workflow tasks (an NDE shows up here; the workflow itself stays Running)."""
    return [
        e.workflow_task_failed_event_attributes.failure.message
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    ]


def scheduled_activities(history: WorkflowHistory) -> List[str]:
    return [
        e.activity_task_scheduled_event_attributes.activity_type.name
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ]


async def replay(history: WorkflowHistory, workflow_cls: Type = GraphInterpreterWorkflow) -> Optional[BaseException]:
    """Replay a history against workflow code. Returns None on success, the failure otherwise."""
    replayer = Replayer(workflows=[workflow_cls], workflow_runner=RUNNER)
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    return result.replay_failure


def is_nde(err: Optional[BaseException]) -> bool:
    return isinstance(err, workflow.NondeterminismError)


async def assert_replays(history: WorkflowHistory, workflow_cls: Type = GraphInterpreterWorkflow) -> None:
    err = await replay(history, workflow_cls)
    assert err is None, f"expected replay to pass with {workflow_cls.__name__}, got: {err}"


async def assert_nde(history: WorkflowHistory, workflow_cls: Type) -> str:
    err = await replay(history, workflow_cls)
    assert is_nde(err), f"expected NDE with {workflow_cls.__name__}, got: {err!r}"
    return str(err)


def save_history(name: str, history: WorkflowHistory) -> None:
    """Capture a history for the golden replay gate (only when UPDATE_HISTORIES=1)."""
    if os.environ.get("UPDATE_HISTORIES") == "1":
        HISTORIES.mkdir(exist_ok=True)
        doc = {"workflow_id": history.workflow_id, "history": json.loads(history.to_json())}
        (HISTORIES / f"{name}.json").write_text(json.dumps(doc, indent=1))
