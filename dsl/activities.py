"""Activities: the only bridge from the interpreter workflow to graphs, the ledger and side effects."""

from typing import Any, Dict, Mapping, Optional, Tuple

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from dsl.models import (
    WORKFLOW_NAME,
    ExecuteInput,
    FetchInput,
    GotoInput,
    JourneyInput,
    NodeSpec,
    ParticipantInput,
    Position,
)
from dsl.ports import GraphRepository, JourneyLedger, NodeCall, NodeHandler, StepRecord, config_error
from dsl.resolve import resolve_next


class DslActivities:
    def __init__(
        self,
        graphs: GraphRepository,
        ledger: JourneyLedger,
        handlers: Mapping[str, NodeHandler],
        client: Optional[Client] = None,
        task_queue: str = "",
    ) -> None:
        self.graphs = graphs
        self.ledger = ledger
        self.handlers = handlers
        self.client = client  # only needed for start_participant
        self.task_queue = task_queue

    @activity.defn
    async def load_position(self, journey_id: str) -> Position:
        return self.ledger.position(journey_id) or Position()

    @activity.defn
    async def fetch_next_node(self, inp: FetchInput) -> NodeSpec:
        """Commit where the journey stands, then decide the next node on the latest published graph."""
        completed = self.ledger.commit_position(inp.journey_id, inp.position)
        latest = self.graphs.latest(inp.graph_id)
        version = inp.position.graph_version
        previous = self.graphs.get(inp.graph_id, version) if version else None
        return resolve_next(latest, previous, inp.position, completed, inp.goto)

    @activity.defn
    async def execute_node(self, inp: ExecuteInput) -> str:
        """Run one node's business logic once per step.

        fetch_next_node decides WHICH node runs. The config is resolved here from the latest graph, so a
        published fix reaches a retry; a node removed since the fetch runs as it was fetched.
        """
        done = self.ledger.step_outcome(inp.journey_id, inp.step_no)
        if done is not None:
            return done  # already ran: never repeat it
        node, version = self._node(inp)
        handler = self.handlers.get(node.get("kind", ""))
        if handler is None:
            raise config_error(f"node {inp.node_id}: unknown kind {node.get('kind')!r}")
        config = dict(node.get("config", {}))
        call = NodeCall(
            journey_id=inp.journey_id, step_no=inp.step_no, node_id=inp.node_id, kind=node["kind"],
            config=config, graph_version=version,
            idempotency_key=f"{inp.journey_id}:{inp.step_no}:{inp.node_id}",
        )
        outcome = await handler(call)
        self.ledger.record_step(inp.journey_id, StepRecord(
            step_no=inp.step_no, node_id=inp.node_id, kind=call.kind, outcome=outcome, graph_version=version, config=config,
        ))
        return outcome

    def _node(self, inp: ExecuteInput) -> Tuple[Dict[str, Any], int]:
        latest = self.graphs.latest(inp.graph_id)
        if inp.node_id in latest["nodes"]:
            return latest["nodes"][inp.node_id], latest["version"]
        if inp.graph_version:
            fetched = self.graphs.get(inp.graph_id, inp.graph_version)["nodes"].get(inp.node_id)
            if fetched is not None:
                return fetched, inp.graph_version
        raise config_error(f"node {inp.node_id} not found")

    @activity.defn
    async def validate_goto(self, inp: GotoInput) -> bool:
        nodes = self.graphs.latest(inp.graph_id)["nodes"]
        return inp.target in nodes and inp.target not in self.ledger.completed_nodes(inp.journey_id)

    @activity.defn
    async def start_participant(self, inp: ParticipantInput) -> str:
        """Start one participant's journey as a top-level workflow; a repeated start counts as done."""
        workflow_id = f"{inp.journey_id}:{inp.participant}"
        try:
            await self.client.start_workflow(
                WORKFLOW_NAME, JourneyInput(journey_id=workflow_id, graph_id=inp.graph_id),
                id=workflow_id, task_queue=self.task_queue,
            )
        except WorkflowAlreadyStartedError:
            return "already-started"
        return "started"

    def all(self):
        return [self.load_position, self.fetch_next_node, self.execute_node, self.validate_goto, self.start_participant]
