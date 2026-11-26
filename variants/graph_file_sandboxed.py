"""Anti-pattern: graph loaded from a file when the workflow module itself is imported.

The sandbox re-imports workflow modules and blocks file access, so a worker with this workflow fails
validation at start-up (RestrictedWorkflowAccessError) instead of producing an NDE later.
"""

import json
import os
from pathlib import Path

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from dsl.models import JourneyInput, Position
    from dsl.workflow import WORKFLOW_NAME
    from variants.interpreters import _GraphInWorkflow

GRAPH = json.loads(Path(os.environ["DSL_GRAPH_FILE"]).read_text())


@workflow.defn(name=WORKFLOW_NAME)
class GraphFileSandboxed(_GraphInWorkflow):
    @workflow.run
    async def run(self, inp: JourneyInput) -> Position:
        return await self.interpret(inp)

    def graph(self):
        return GRAPH
