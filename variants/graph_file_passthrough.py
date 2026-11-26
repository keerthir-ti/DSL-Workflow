"""Anti-pattern: graph loaded from a file on import, through a pass-through module (bypasses the sandbox)."""

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from dsl.models import JourneyInput, Position
    from dsl.workflow import WORKFLOW_NAME
    from variants import graph_file_loader
    from variants.interpreters import _GraphInWorkflow


@workflow.defn(name=WORKFLOW_NAME)
class GraphFilePassThrough(_GraphInWorkflow):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    def graph(self):
        return graph_file_loader.GRAPH
