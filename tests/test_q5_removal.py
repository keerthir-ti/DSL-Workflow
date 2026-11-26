"""Q5. Removing a node or activity that some running journeys already executed and others haven't reached.

Three different things can be "removed":
- a node from the graph (data)                  -> safe, no patch
- an activity registration from the worker     -> replay is fine; an in-flight call waits until it is back
- an activity call from the interpreter code   -> NDE unless patched (e.g. merging two activities for cost)
"""

from tests.helpers import (
    act,
    assert_nde,
    assert_replays,
    graph,
    scheduled_activities,
    send,
    sig,
    wait_at,
    wait_until,
)
from variants.interpreters import MergedFetchExecutePatched, MergedFetchExecuteUnpatched


def v1():
    return graph(
        "n1",
        n1=act("s1"),
        s1=sig("a", "n3"),
        n3=act("s2", kind="update_record", field="status", delay_seconds=2),
        s2=sig("b", "n5"),
        n5=act(None),
    )


def v2_without_n3():
    g = v1()
    del g["nodes"]["n3"]
    g["nodes"]["s1"]["next"]["received"] = "s2"
    return g


async def test_remove_node_from_graph_for_journeys_before_on_and_past_it(local):
    local.store.publish(v1())
    async with local.worker():
        before = await local.start("before")
        past = await local.start("past")
        on = await local.start("on")
        for handle in (before, past, on):
            await wait_at(handle, "s1")
        await send(past, "a")
        await wait_at(past, "s2")  # n3 already ran
        await send(on, "a")
        await wait_at(on, "n3")  # n3 is in flight (2 s side effect)

        local.store.publish(v2_without_n3())  # remove n3 while all three are running

        await send(before, "a")
        await wait_at(on, "s2")
        for handle in (before, past, on):
            await wait_at(handle, "s2")
            await send(handle, "b")
            await handle.result()

    assert local.effects("before") == ["n1", "n5"]  # never reaches the removed node
    assert local.effects("past") == ["n1", "n3", "n5"]  # ran it before removal, carries on
    assert local.effects("on") == ["n1", "n3", "n5"]  # in-flight call completes, then moves on
    for handle in (before, past, on):
        await assert_replays(await handle.fetch_history())


async def test_unregistered_activity_in_flight_waits_until_registered(local):
    local.store.publish(graph("n1", n1=act("n2"), n2=act(None)))
    without_execute = [a for a in local.dsl.all() if a.__name__ != "execute_node"] + local.extra.all()

    async with local.worker(activities=without_execute):
        handle = await local.start("unregistered")

        async def failing_attempts():
            pending = (await handle.describe()).raw_description.pending_activities
            return pending and pending[0].attempt >= 3 and "not registered" in pending[0].last_failure.message

        await wait_until(failing_attempts, timeout=30)
    assert local.effects("unregistered") == []  # nothing ran, journey is waiting, not failed

    async with local.worker():  # the activity is registered again
        await handle.result()
    assert local.effects("unregistered") == ["n1", "n2"]

    # Replay never runs activities, so removing a registration cannot break replay
    await assert_replays(await handle.fetch_history())


async def test_merge_fetch_and_execute_needs_patch_and_halves_activity_calls(ts):
    ts.store.publish(graph("n1", n1=act("n2"), n2=act("s"), s=sig("go", "n4"), n4=act(None)))
    async with ts.worker():
        old = await ts.start("merge-old")
        await wait_at(old, "s")
        parked = await old.fetch_history()

    await assert_nde(parked, MergedFetchExecuteUnpatched)
    await assert_replays(parked, MergedFetchExecutePatched)

    async with ts.worker(MergedFetchExecutePatched):
        await send(old, "go")
        await old.result()  # the old journey finishes on its old branch
        new = await ts.start("merge-new")
        await wait_at(new, "s")
        await send(new, "go")
        await new.result()

    old_calls = scheduled_activities(await old.fetch_history())
    new_calls = scheduled_activities(await new.fetch_history())
    assert "fetch_and_execute" not in old_calls
    assert ts.effects("merge-old") == ts.effects("merge-new") == ["n1", "n2", "n4"]
    # load_position + 5 fetches (4 nodes + END) + 3 executes = 9 calls; merged: load_position + 5 = 6
    assert (len(old_calls), len(new_calls)) == (9, 6)
