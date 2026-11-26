"""Q7. History growth: where to continue-as-new, and does each new run pick up the latest graph (and code)?

Continue-as-new happens only at a node boundary, right after fetch_next_node committed the position
to the ledger. The new run gets ids only, re-reads its position and fetches from the latest graph.
"""

import asyncio

import pytest

from tests.helpers import act, assert_replays, event_types, fanout, graph, save_history, send, sig, timer, wait_at, wait_until
from tests.probe import runs

DAY = 24 * 3600


def chain(n):
    nodes = {f"n{i}": act(f"n{i + 1}" if i < n else None) for i in range(1, n + 1)}
    return graph("n1", **nodes)


async def test_continue_as_new_every_n_hops_bounds_history(ts):
    ts.store.publish(chain(6))
    async with ts.worker():
        unbounded = await ts.start("one-run")
        await unbounded.result()
        bounded = await ts.start("bounded", hops_per_run=2)
        await bounded.result()

    assert runs(ts.jid("one-run")) == 1
    assert runs(ts.jid("bounded")) == 3  # 6 nodes, 2 per run
    assert ts.effects("bounded") == ts.effects("one-run") == [f"n{i}" for i in range(1, 7)]  # nothing re-run
    one_run, last_run = await unbounded.fetch_history(), await bounded.fetch_history()
    assert event_types(last_run)[0] == "WORKFLOW_EXECUTION_STARTED"  # a run starts with an empty history
    assert len(last_run.events) < len(one_run.events) / 2
    await assert_replays(last_run)
    save_history("q7_continue_as_new_last_run", last_run)


async def test_new_run_follows_latest_graph(ts):
    ts.store.publish(graph("n1", n1=act("s"), s=sig("go", "n3"), n3=act(None, kind="update_record", field="a")))
    async with ts.worker():
        handle = await ts.start("latest", hops_per_run=2)
        await wait_at(handle, "s")
        g2 = graph("n1", n1=act("s"), s=sig("go", "n2b"), n2b=act("n3"), n3=act(None, kind="update_record", field="b"))
        ts.store.publish(g2)  # published while run 1 waits
        await send(handle, "go")  # hop 2 -> continue-as-new -> run 2 starts from the ledger position
        await handle.result()

    assert runs(ts.jid("latest")) == 2
    assert ts.effects("latest") == ["n1", "n2b", "n3"]
    assert ts.ledger.steps(ts.jid("latest"))[-1]["config"]["field"] == "b"


async def test_long_timer_is_chunked_and_adopts_reconfigured_wait(ts):
    ts.store.publish(graph("t", t=timer(3 * DAY, "n2"), n2=act(None)))
    async with ts.worker():
        handle = await ts.start("chunked", timer_chunk_seconds=DAY)
        await ts.env.sleep(1.5 * DAY)
        ts.store.publish(graph("t", t=timer(2 * DAY, "n2"), n2=act(None)))  # wait shortened mid-wait
        await handle.result()
    first_run = await ts.client.get_workflow_handle(handle.id, run_id=handle.result_run_id).fetch_history()
    last_run = await ts.handle("chunked").fetch_history()
    elapsed = last_run.events[-1].event_time.seconds - first_run.events[0].event_time.seconds

    assert 2 * DAY <= elapsed < 2.5 * DAY  # took the new 2-day length, not the original 3 days
    assert runs(ts.jid("chunked")) == 3  # one run per day chunk
    assert ts.effects("chunked") == ["n2"]


async def test_signal_received_before_continue_as_new_is_not_lost(ts):
    ts.store.publish(graph("t", t=timer(2 * DAY, "s"), s=sig("go", "n"), n=act(None)))
    async with ts.worker():
        handle = await ts.start("carry", timer_chunk_seconds=DAY)
        await wait_at(handle, "t")
        await send(handle, "go")  # arrives during run 1, consumed by the signal node in run 3
        await handle.result()

    assert runs(ts.jid("carry")) >= 2
    assert ts.effects("carry") == ["n"]


@pytest.mark.parametrize(
    "config, expected_child_status",
    [
        ({"close_policy": "TERMINATE"}, "TERMINATED"),  # child workflow, default-style close policy
        ({"close_policy": "ABANDON"}, "RUNNING"),
        ({"via": "activity"}, "RUNNING"),  # participant started by an activity as a top-level workflow
    ],
)
async def test_fanout_participants_across_parent_continue_as_new(local, config, expected_child_status):
    local.store.publish(graph("c1", graph_id="child", c1=sig("never", None)))
    parent = f"fan-{expected_child_status}-{'-'.join(config.values())}"
    local.store.publish(graph("f", f=fanout(["p1"], "child", "s", **config), s=sig("go", None)))
    async with local.worker():
        handle = await local.start(parent, hops_per_run=1)  # continue-as-new right after the fan-out
        child = local.client.get_workflow_handle(f"{local.jid(parent)}:p1")
        await wait_until(lambda: runs(local.jid(parent)) >= 2)
        await wait_at(handle, "s")

        async def child_status():
            status = (await child.describe()).status.name
            return status if status == expected_child_status else None

        assert await wait_until(child_status) == expected_child_status
        await handle.terminate()
        if expected_child_status == "RUNNING":
            await child.terminate()


async def test_signals_arriving_during_continue_as_new_are_not_lost(local):
    """Five signal nodes, a continue-as-new after each; all five signals are sent at once while it rolls over."""
    names = [f"e{i}" for i in range(1, 6)]
    nodes = {f"s{i}": sig(f"e{i}", f"s{i + 1}" if i < 5 else "done") for i in range(1, 6)}
    local.store.publish(graph("s1", **nodes, done=act(None)))
    async with local.worker():
        handle = await local.start("race", hops_per_run=1)
        await asyncio.gather(*(send(handle, name) for name in reversed(names)))
        await handle.result()

    assert runs(local.jid("race")) == 6
    assert local.effects("race") == ["done"]
