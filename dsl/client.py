"""Calls the application makes. The workflow id is the journey id, so every call is addressed by it."""

from typing import Any, Dict

from temporalio.client import Client, WorkflowHandle

from dsl.models import WORKFLOW_NAME, Event, GotoMode, GotoRequest, JourneyInput


async def start_journey(client: Client, journey_id: str, graph_id: str, task_queue: str, **options: Any) -> WorkflowHandle:
    """Start one journey. options: hops_per_run, timer_chunk_seconds (continue-as-new tuning)."""
    return await client.start_workflow(
        WORKFLOW_NAME, JourneyInput(journey_id=journey_id, graph_id=graph_id, **options),
        id=journey_id, task_queue=task_queue,
    )


async def send_event(client: Client, journey_id: str, name: str, payload: Any = None) -> None:
    """An external event a signal node waits for."""
    await client.get_workflow_handle(journey_id).signal("event", Event(name, payload))


async def nudge(client: Client, journey_id: str) -> None:
    """After a publish: a waiting node re-reads its definition from the latest graph."""
    await client.get_workflow_handle(journey_id).signal("nudge")


async def move_to_node(client: Client, journey_id: str, node_id: str, fresh_history: bool = False) -> str:
    """Move a running journey to node_id. Returns "accepted" or "rejected" (unknown or already-run node)."""
    mode = GotoMode.CONTINUE_AS_NEW if fresh_history else GotoMode.IN_PLACE
    return await client.get_workflow_handle(journey_id).execute_update("goto", GotoRequest(node_id, mode))


async def resume(client: Client, journey_id: str) -> str:
    """Release a journey parked as NEEDS_ATTENTION (after the configuration is fixed)."""
    return await client.get_workflow_handle(journey_id).execute_update("resume")


async def state(client: Client, journey_id: str) -> Dict[str, Any]:
    """Status, current node, step and graph version."""
    return await client.get_workflow_handle(journey_id).query("state")
