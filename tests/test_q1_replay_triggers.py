"""Q1. When does replay happen? The probe counts how often a run is rebuilt from its history.

Runs on the local dev server with a normal (cached) worker unless the test changes it.
"""

import uuid

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest

from tests.helpers import act, event_types, graph, send, sig, wait_at
from tests.probe import REBUILDS, rebuilds, runs


def journey():
    return graph("n1", n1=act("n2"), n2=act("s"), s=sig("go", "n4"), n4=act(None))


async def test_baseline_cached_worker_never_replays(local):
    local.store.publish(journey())
    async with local.worker():
        handle = await local.start("baseline")
        await wait_at(handle, "s")
        await send(handle, "go")
        await handle.result()
    assert rebuilds(local.jid("baseline")) == 0  # 9 workflow tasks, all served from the cache


async def test_worker_restart_or_crash_replays_full_history(local):
    local.store.publish(journey())
    async with local.worker():
        handle = await local.start("restart")
        await wait_at(handle, "s")
    async with local.worker():  # a new worker process (deploy, crash, scale-in)
        await send(handle, "go")
        await handle.result()
    assert rebuilds(local.jid("restart")) == 1


async def test_cache_eviction_replays_on_every_task(local):
    local.store.publish(journey())
    async with local.worker(max_cached_workflows=0):  # = every workflow evicted after each task
        handle = await local.start("evicted")
        await wait_at(handle, "s")
        before_signal = rebuilds(local.jid("evicted"))
        await send(handle, "go")
        await handle.result()
    assert before_signal >= 5
    assert rebuilds(local.jid("evicted")) > before_signal  # every new task rebuilt the run again


async def test_query_on_uncached_workflow_replays(local):
    local.store.publish(journey())
    async with local.worker(max_cached_workflows=0):
        handle = await local.start("query")
        await wait_at(handle, "s")
        before = rebuilds(local.jid("query"))
        await handle.query("state")
        assert rebuilds(local.jid("query")) == before + 1
        await handle.terminate()


async def test_query_on_closed_workflow_replays(local):
    local.store.publish(journey())
    async with local.worker():
        handle = await local.start("closed")
        await wait_at(handle, "s")
        await send(handle, "go")
        await handle.result()
        assert rebuilds(local.jid("closed")) == 0
        state = await handle.query("state")
    assert state["status"] == "COMPLETED"
    assert rebuilds(local.jid("closed")) == 1  # a closed workflow is never cached: always a replay


async def test_reset_replays_history_up_to_the_reset_point(local):
    local.store.publish(journey())
    async with local.worker():
        handle = await local.start("reset")
        await wait_at(handle, "s")
        history = await handle.fetch_history()
        after_n1 = [e.event_id for e in history.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED][3]
        response = await local.client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=local.client.namespace,
                workflow_execution=WorkflowExecution(workflow_id=handle.id, run_id=history.run_id),
                reason="replay trigger test",
                workflow_task_finish_event_id=after_n1,
                request_id=str(uuid.uuid4()),
            )
        )
        reset_run = local.client.get_workflow_handle(handle.id, run_id=response.run_id)
        await wait_at(reset_run, "s")
        await send(reset_run, "go")
        await reset_run.result()
    assert rebuilds(local.jid("reset"), response.run_id) == 1  # the new run replays the copied prefix
    assert local.effects("reset") == ["n1", "n2", "n4"]  # nothing ran twice


async def test_continue_as_new_starts_with_nothing_to_replay(local):
    local.store.publish(graph("n1", n1=act("n2"), n2=act("n3"), n3=act(None)))
    async with local.worker():
        handle = await local.start("can", hops_per_run=1)
        await handle.result()
    workflow_id = local.jid("can")
    assert runs(workflow_id) == 3
    assert rebuilds(workflow_id) == 0
    assert all(n == 0 for (wid, _), n in REBUILDS.items() if wid == workflow_id)
    last = await local.handle("can").fetch_history()
    assert event_types(last)[0] == "WORKFLOW_EXECUTION_STARTED" and len(last.events) < 30
