"""Test-only replay probe: counts how often each workflow run is rebuilt from history.

A run is "rebuilt" when the worker creates a fresh workflow instance while history already has
events, i.e. execute_workflow starts with is_replaying() == True. A brand-new run starts with False.
The probe only observes; it issues no commands, so it cannot affect determinism.
"""

from collections import Counter
from typing import Any, Optional, Type

from temporalio import workflow
from temporalio.worker import ExecuteWorkflowInput, Interceptor, WorkflowInboundInterceptor, WorkflowInterceptorClassInput

REBUILDS: Counter = Counter()  # (workflow_id, run_id) -> number of rebuilds from history
STARTS: Counter = Counter()  # (workflow_id, run_id) -> number of fresh (non-replay) starts


class _Inbound(WorkflowInboundInterceptor):
    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        info = workflow.info()
        key = (info.workflow_id, info.run_id)
        (REBUILDS if workflow.unsafe.is_replaying() else STARTS)[key] += 1
        return await super().execute_workflow(input)


class ReplayProbe(Interceptor):
    def workflow_interceptor_class(self, input: WorkflowInterceptorClassInput) -> Optional[Type[WorkflowInboundInterceptor]]:
        return _Inbound


def rebuilds(workflow_id: str, run_id: Optional[str] = None) -> int:
    return sum(n for (wid, rid), n in REBUILDS.items() if wid == workflow_id and (run_id is None or rid == run_id))


def runs(workflow_id: str) -> int:
    return len({rid for (wid, rid) in STARTS if wid == workflow_id})
