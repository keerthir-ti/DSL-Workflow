"""Q9. Reproduce a live NDE after an incompatible deploy, then handle it.

Reproduce:
  R1 journey runs n1 on code v1 and parks on signal node s
  R2 the v1 worker is replaced by v2 (extra activity before every fetch, NOT patched)
  R3 the signal arrives, v2 replays the history and fails the workflow task: stuck, still Running
  R4 the same history replayed offline with Replayer on v2 fails too: a CI gate would have blocked it
Handle (each from the stuck state of R3):
  H1 roll back to v1 · H2 deploy v2 behind workflow.patched() · H3 reset · H4 terminate + start
"""

import uuid

import pytest
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError

from dsl.models import JourneyInput
from dsl.workflow import WORKFLOW_NAME, GraphInterpreterWorkflow
from tests.helpers import (
    act,
    assert_nde,
    assert_replays,
    event_types,
    graph,
    send,
    sig,
    wait_at,
    wait_until,
    workflow_task_failures,
)
from variants.interpreters import ExtraActivityPatched, ExtraActivityUnpatched

V1, V2_UNPATCHED, V2_PATCHED = GraphInterpreterWorkflow, ExtraActivityUnpatched, ExtraActivityPatched


async def reproduce(ctx, name):
    """R1-R4. Returns the stuck journey's handle and the history captured before the bad deploy."""
    ctx.store.publish(graph("n1", n1=act("s"), s=sig("go", "n3"), n3=act(None)))
    async with ctx.worker(V1):  # R1
        handle = await ctx.start(name)
        await wait_at(handle, "s")
        parked = await handle.fetch_history()

    async with ctx.worker(V2_UNPATCHED):  # R2 + R3
        await send(handle, "go")
        failures = await wait_until(lambda: _failures(handle), timeout=30)
        assert any("Nondeterminism" in f for f in failures)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING  # stuck, not failed
        assert ctx.effects(name) == ["n1"]  # n3 never ran

    await assert_nde(parked, V2_UNPATCHED)  # R4: caught offline, before deploy
    return handle, parked


async def _failures(handle):
    return workflow_task_failures(await handle.fetch_history())


async def test_h1_roll_back(local):
    handle, _ = await reproduce(local, "h1")
    async with local.worker(V1):
        await handle.result()
    assert local.effects("h1") == ["n1", "n3"]
    await assert_replays(await handle.fetch_history(), V1)


async def test_h2_fix_forward_with_patch(local):
    handle, parked = await reproduce(local, "h2")
    await assert_replays(parked, V2_PATCHED)  # gate passes with the patched code
    async with local.worker(V2_PATCHED):
        await handle.result()  # old journey heals on the old branch
        fresh = await local.start("h2-new")  # new journey takes the new branch
        await wait_at(fresh, "s")
        await send(fresh, "go")
        await fresh.result()

    assert local.effects("h2") == ["n1", "n3"]
    assert local.jid("h2") not in local.extra.audit_log
    assert local.jid("h2-new") in local.extra.audit_log
    await assert_replays(await handle.fetch_history(), V2_PATCHED)
    await assert_replays(await fresh.fetch_history(), V2_PATCHED)


async def test_h3_reset_to_before_the_divergence(local):
    handle, _ = await reproduce(local, "h3")
    stuck = await handle.fetch_history()
    first_task = next(e.event_id for e in stuck.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
    response = await local.client.workflow_service.reset_workflow_execution(
        ResetWorkflowExecutionRequest(
            namespace=local.client.namespace,
            workflow_execution=WorkflowExecution(workflow_id=handle.id, run_id=stuck.run_id),
            reason="NDE after deploy",
            workflow_task_finish_event_id=first_task,
            request_id=str(uuid.uuid4()),
        )
    )
    reset_run = local.client.get_workflow_handle(handle.id, run_id=response.run_id)
    async with local.worker(V2_UNPATCHED):  # the new code runs the journey from the reset point
        await reset_run.result()

    reset_history = await reset_run.fetch_history()
    assert "WORKFLOW_EXECUTION_SIGNALED" in event_types(reset_history)  # reset re-applied the "go" signal
    assert local.effects("h3") == ["n1", "n3"]  # position from the ledger: n1 not run again
    await assert_replays(reset_history, V2_UNPATCHED)


async def test_h4_terminate_and_start_from_ledger_position(local):
    handle, _ = await reproduce(local, "h4")
    await handle.terminate(reason="NDE after deploy")
    with pytest.raises(RPCError):
        await send(handle, "go")  # a signal sent between terminate and start is lost

    async with local.worker(V2_UNPATCHED):
        new = await local.client.start_workflow(
            WORKFLOW_NAME, JourneyInput(journey_id=local.jid("h4"), graph_id="g"),
            id=local.jid("h4"), task_queue=local.task_queue,
        )
        await wait_at(new, "s")  # resumes at the ledger position; the earlier "go" is gone
        await send(new, "go")  # the event has to be sent again
        await new.result()

    assert local.effects("h4") == ["n1", "n3"]
    await assert_replays(await new.fetch_history(), V2_UNPATCHED)
