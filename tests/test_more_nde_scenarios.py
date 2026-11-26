"""Interpreter code changes: each one gives an NDE on an old history, and the same change behind
workflow.patched() replays the old history while new journeys take the new path.
"""

from temporalio.api.enums.v1 import EventType

from dsl.models import GotoRequest
from dsl.workflow import GraphInterpreterWorkflow
from tests.helpers import act, assert_nde, assert_replays, fanout, graph, park, scheduled_activities, send, sig, terminate
from tests.probe import runs
from variants import shared_state
from variants.interpreters import (
    ActivityRenamed,
    ActivityRenamedPatched,
    ClientFlagFromActivity,
    ContinueAsNewEveryHop,
    ContinueAsNewEveryHopPatched,
    EventHandlerCountsEvents,
    FanoutReversedOrder,
    FanoutReversedOrderPatched,
    FanoutViaActivity,
    FanoutViaActivityPatched,
    GotoWithoutValidation,
    GotoWithoutValidationPatched,
    UpsertNodeAttribute,
    UpsertNodeAttributePatched,
)

SIMPLE = graph("n1", n1=act("n2"), n2=act("s"), s=sig("go", None))


async def test_activity_renamed(ts):
    _, old = await park(ts, "rename-old", "s", graph=SIMPLE)
    await assert_nde(old, ActivityRenamed)
    await assert_replays(old, ActivityRenamedPatched)
    _, new = await park(ts, "rename-new", "s", ActivityRenamedPatched, graph=SIMPLE)
    assert "run_node" in scheduled_activities(new) and "execute_node" not in scheduled_activities(new)


async def test_continue_as_new_condition_changed(ts):
    _, old = await park(ts, "can-old", "s", graph=SIMPLE)
    await assert_nde(old, ContinueAsNewEveryHop)
    await assert_replays(old, ContinueAsNewEveryHopPatched)
    ts.store.publish(graph("n1", n1=act("n2"), n2=act(None)))
    async with ts.worker(ContinueAsNewEveryHopPatched):
        await (await ts.start("can-new")).result()
    assert runs(ts.jid("can-new")) == 2  # 2 nodes: continue-as-new after the first, the second ends the journey


async def test_activity_removed_from_update_handler(local):
    g = graph("n1", n1=act("s"), s=sig("go", "n3"), n3=act(None), nX=act("n3"))
    handle, _ = await park(local, "goto-old", "s", graph=g)
    async with local.worker(GraphInterpreterWorkflow):
        assert await handle.execute_update("goto", GotoRequest("nX")) == "accepted"  # runs validate_goto
        await handle.result()
    old = await handle.fetch_history()
    await assert_nde(old, GotoWithoutValidation)
    await assert_replays(old, GotoWithoutValidationPatched)

    new, _ = await park(local, "goto-new", "s", GotoWithoutValidationPatched, graph=g)
    async with local.worker(GotoWithoutValidationPatched):
        assert await new.execute_update("goto", GotoRequest("nX")) == "accepted"
        await new.result()
    assert "validate_goto" not in scheduled_activities(await new.fetch_history())


async def test_signal_handler_changed_without_commands_is_safe(ts):
    handle, _ = await park(ts, "signal", "s", graph=graph("n1", n1=act("s"), s=sig("go", "n3"), n3=act(None)))
    async with ts.worker(GraphInterpreterWorkflow):
        await send(handle, "go")
        await handle.result()
    await assert_replays(await handle.fetch_history(), EventHandlerCountsEvents)


# Fan-out runs on the local server: child workflows left on the time-skipping server stop its time skipping.
def fanout_graph():
    return graph("f", f=fanout(["p1", "p2", "p3"], "child", "s"), s=sig("go", None))


def child_starts(history):
    return [
        e.start_child_workflow_execution_initiated_event_attributes.workflow_id.rsplit(":", 1)[-1]
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
    ]


async def test_fanout_child_to_activity(local):
    local.store.publish(graph("c1", graph_id="child", c1=sig("never", None)))
    old_handle, old = await park(local, "via-old", "s", graph=fanout_graph())
    await assert_nde(old, FanoutViaActivity)
    await assert_replays(old, FanoutViaActivityPatched)
    new_handle, new = await park(local, "via-new", "s", FanoutViaActivityPatched, graph=fanout_graph())
    assert child_starts(new) == [] and scheduled_activities(new).count("start_participant") == 3
    await terminate(local, old_handle.id, new_handle.id,
                    *(f"{h.id}:{p}" for h in (old_handle, new_handle) for p in ("p1", "p2", "p3")))


async def test_child_start_order_changed(local):
    local.store.publish(graph("c1", graph_id="child", c1=sig("never", None)))
    old_handle, old = await park(local, "order-old", "s", graph=fanout_graph())
    await assert_nde(old, FanoutReversedOrder)
    await assert_replays(old, FanoutReversedOrderPatched)
    new_handle, new = await park(local, "order-new", "s", FanoutReversedOrderPatched, graph=fanout_graph())
    assert child_starts(old) == ["p1", "p2", "p3"] and child_starts(new) == ["p3", "p2", "p1"]
    await terminate(local, old_handle.id, new_handle.id,
                    *(f"{h.id}:{p}" for h in (old_handle, new_handle) for p in ("p1", "p2", "p3")))


async def test_search_attribute_upsert_added(local):
    old_handle, old = await park(local, "upsert-old", "s", graph=SIMPLE)
    await assert_nde(old, UpsertNodeAttribute)
    await assert_replays(old, UpsertNodeAttributePatched)
    new_handle, new = await park(local, "upsert-new", "s", UpsertNodeAttributePatched, graph=SIMPLE)
    assert any(e.event_type == EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES for e in new.events)
    await terminate(local, old_handle.id, new_handle.id)


async def test_client_flag_read_through_an_activity_is_safe(ts):
    """Fix for the per-client flag NDE (test_q3): the flag value is recorded in history."""
    shared_state.CLIENT_FLAG["extra_step"] = False
    try:
        _, history = await park(ts, "flag-data", "s", ClientFlagFromActivity, graph=SIMPLE)
        shared_state.CLIENT_FLAG["extra_step"] = True  # the client is switched to the new behaviour
        await assert_replays(history, ClientFlagFromActivity)
    finally:
        shared_state.CLIENT_FLAG["extra_step"] = False
