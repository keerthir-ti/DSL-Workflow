"""Extra activities used only by the test variants."""

from typing import Any, Dict

from temporalio import activity
from temporalio.exceptions import ApplicationError

from dsl.activities import DslActivities
from dsl.models import CONFIG_ERROR, ExecuteInput, FetchInput, NodeSpec, NodeStatus
from variants import shared_state


class VariantActivities:
    def __init__(self, dsl: DslActivities) -> None:
        self.dsl = dsl
        self.audit_log = []

    @activity.defn
    async def get_graph(self, graph_id: str) -> Dict[str, Any]:
        return self.dsl.graphs.latest(graph_id)

    @activity.defn
    async def get_client_flag(self, graph_id: str) -> bool:
        return shared_state.CLIENT_FLAG["extra_step"]

    @activity.defn
    async def audit(self, journey_id: str) -> None:
        self.audit_log.append(journey_id)

    @activity.defn
    async def durable_wait(self, seconds: float) -> str:
        return "fired"

    @activity.defn(name="run_node")
    async def run_node(self, inp: ExecuteInput) -> str:
        return await self.dsl.execute_node(inp)

    @activity.defn
    async def fetch_and_execute(self, inp: FetchInput) -> NodeSpec:
        spec = await self.dsl.fetch_next_node(inp)
        if spec.status == NodeStatus.NODE and spec.type == "activity":
            step_no = inp.position.step_no + 1
            outcome = await self.dsl.execute_node(
                ExecuteInput(inp.journey_id, inp.graph_id, step_no, spec.node_id, spec.graph_version)
            )
            spec.config = {**spec.config, "__outcome": outcome}
        return spec

    @activity.defn
    async def execute_node_with_config(self, spec: NodeSpec) -> str:
        if not spec.config.get("template"):
            raise ApplicationError(f"missing config 'template' (config seen: {spec.config})", type=CONFIG_ERROR)
        return "ok"

    def all(self):
        return [self.get_graph, self.get_client_flag, self.audit, self.durable_wait, self.run_node, self.fetch_and_execute,
                self.execute_node_with_config]
