"""Worker wiring. Pass your own GraphRepository, JourneyLedger and node handlers; nothing else changes."""

from typing import Any, Mapping

from temporalio.client import Client
from temporalio.worker import Worker

from dsl.activities import DslActivities
from dsl.ports import GraphRepository, JourneyLedger, NodeHandler
from dsl.workflow import GraphInterpreterWorkflow

DEFAULT_TASK_QUEUE = "dsl-interpreter"


def create_worker(
    client: Client,
    graphs: GraphRepository,
    ledger: JourneyLedger,
    handlers: Mapping[str, NodeHandler],
    task_queue: str = DEFAULT_TASK_QUEUE,
    **worker_options: Any,
) -> Worker:
    activities = DslActivities(graphs, ledger, handlers, client, task_queue)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[GraphInterpreterWorkflow],
        activities=activities.all(),
        **worker_options,
    )
