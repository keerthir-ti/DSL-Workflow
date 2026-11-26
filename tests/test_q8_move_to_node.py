"""Q8. Move selected running journeys to a newly added node X.

Option A: Update (or Signal) "goto X"        - same run, applied at the next node boundary
Option B: continue-as-new with start node X  - triggered by the same Update with mode=continue_as_new
Option C: terminate + start a new run at X
Measured for each: replay safety, side effects (ledger), in-flight work, signals in transit, history.
"""

import pytest
from temporalio.api.enums.v1 import EventType, PendingActivityState
from temporalio.service import RPCError

from dsl.models import GotoMode, GotoRequest, JourneyInput
from dsl.workflow import WORKFLOW_NAME
from tests.helpers import (
    act,
    assert_replays,
    event_types,
    graph,
    save_history,
    scheduled_activities,
    send,
    sig,
    wait_at,
    wait_until,
)
from tests.probe import runs


def v1(delay=0):
    return graph("n1", n1=act("s1", delay_seconds=delay), s1=sig("go", "n3"), n3=act(None))


def v2(delay=0):
    g = v1(delay)
    g["nodes"]["nX"] = act("n3", kind="update_record", field="new")  # the newly added node
    return g


async def test_a_update_goto_moves_journey_and_is_replay_safe(local):
    local.store.publish(v1())
    async with local.worker():
        moved = await local.start("a-moved")
        other = await local.start("a-other")
        await wait_at(moved, "s1")
        await wait_at(other, "s1")
        local.store.publish(v2())

        assert await moved.execute_update("goto", GotoRequest("nope")) == "rejected"  # validated
        assert await moved.execute_update("goto", GotoRequest("n1")) == "rejected"  # already ran
        assert await moved.execute_update("goto", GotoRequest("nX")) == "accepted"
        await moved.result()
        await send(other, "go")
        await other.result()

    assert local.effects("a-moved") == ["n1", "nX", "n3"]
    assert local.effects("a-other") == ["n1", "n3"]  # not affected
    history = await moved.fetch_history()
    assert "WORKFLOW_EXECUTION_UPDATE_ACCEPTED" in event_types(history)  # recorded, so replay-safe
    await assert_replays(history)
    save_history("q8_update_goto", history)


async def test_a_update_goto_waits_for_in_flight_activity(local):
    local.store.publish(v1(delay=2))
    async with local.worker():
        handle = await local.start("a-inflight")
        await wait_at(handle, "n1")  # n1 is running (2 s)
        local.store.publish(v2(delay=2))
        assert await handle.execute_update("goto", GotoRequest("nX")) == "accepted"
        await handle.result()

    assert local.effects("a-inflight") == ["n1", "nX", "n3"]  # n1 finished once, result kept, then moved
    await assert_replays(await handle.fetch_history())


async def test_a_signal_goto_has_no_validation(local):
    local.store.publish(v1())
    async with local.worker():
        handle = await local.start("a-signal")
        await wait_at(handle, "s1")
        local.store.publish(v2())
        await handle.signal("goto_signal", "nope")  # accepted silently by the server
        await wait_at(handle, "s1", status="NEEDS_ATTENTION")  # orphaned target: parked
        await handle.signal("goto_signal", "nX")
        await handle.result()

    assert local.effects("a-signal") == ["n1", "nX", "n3"]
    await assert_replays(await handle.fetch_history())


async def test_b_continue_as_new_with_start_node(local):
    local.store.publish(v1())
    async with local.worker():
        handle = await local.start("b-can")
        await wait_at(handle, "s1")
        local.store.publish(v2())
        assert await handle.execute_update("goto", GotoRequest("nX", mode=GotoMode.CONTINUE_AS_NEW)) == "accepted"
        await handle.result()

    assert runs(local.jid("b-can")) == 2
    assert local.effects("b-can") == ["n1", "nX", "n3"]
    first = await local.client.get_workflow_handle(handle.id, run_id=handle.result_run_id).fetch_history()
    second = await local.handle("b-can").fetch_history()
    assert event_types(first)[-1] == "WORKFLOW_EXECUTION_CONTINUED_AS_NEW"
    assert "execute_node" not in scheduled_activities(second)[:2]  # new run starts at nX, not at n1
    await assert_replays(first)
    await assert_replays(second)


async def test_c_terminate_and_start_loses_in_flight_result_and_signals(local):
    local.store.publish(v1(delay=2))
    async with local.worker():
        handle = await local.start("c-term")
        await wait_until(lambda: _activity_started(handle))  # n1 in flight on the worker
        local.store.publish(v2(delay=2))
        await handle.terminate(reason="move to nX")

        with pytest.raises(RPCError):
            await send(handle, "go")  # a signal in transit has nowhere to go: lost

        new = await local.client.start_workflow(
            WORKFLOW_NAME, JourneyInput(journey_id=local.jid("c-term"), graph_id="g", start_node="nX"),
            id=local.jid("c-term"), task_queue=local.task_queue,
        )
        await new.result()
        await wait_until(lambda: "n1" in local.effects("c-term"))  # the orphaned call still completes

    old = await local.client.get_workflow_handle(handle.id, run_id=handle.result_run_id).fetch_history()
    assert event_types(old)[-1] == "WORKFLOW_EXECUTION_TERMINATED"
    assert not _execute_node_completed(old)  # n1's result never reached the old run
    # the side effect still happened, outside any running workflow; only the ledger stops a re-run
    assert sorted(local.effects("c-term")) == ["n1", "n3", "nX"]
    await assert_replays(await new.fetch_history())


def _execute_node_completed(history):
    scheduled = {
        e.event_id for e in history.events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        and e.activity_task_scheduled_event_attributes.activity_type.name == "execute_node"
    }
    return any(
        e.activity_task_completed_event_attributes.scheduled_event_id in scheduled
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
    )


async def _activity_started(handle):
    pending = (await handle.describe()).raw_description.pending_activities
    return bool(pending) and pending[0].state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
