"""Q2. Graph changes mid-run, next node fetched through an activity: is replay of the old history safe?

Each scenario:
1. runs a journey on graph v1 until it is parked on the signal node n2 (n1 already ran);
2. captures the history, publishes v2 with one kind of change, and replays the OLD history -> must pass;
3. continues the live journey on a worker with the cache disabled (every task is a full replay);
4. checks it followed v2 from its position and never re-ran a node; replays the FINAL history.
"""

import copy

import pytest

from tests.helpers import act, assert_replays, event_types, graph, save_history, send, sig, timer, wait_at
from tests.probe import rebuilds


def v1():
    return graph(
        "n1",
        n1=act("n2", kind="send_email", template="welcome"),
        n2=sig("go", "n3"),
        n3=act("n4", kind="update_record", field="status"),
        n4=act(None),
    )


def change(fn):
    def apply():
        g = copy.deepcopy(v1())
        fn(g["nodes"], g)
        return g
    return apply


def add_ahead(n, g):
    n["n2"]["next"]["received"] = "nX"
    n["nX"] = act("n3")


def add_behind(n, g):
    n["n1"]["next"]["ok"] = "nB"
    n["nB"] = act("n2")


def remove_passed(n, g):
    del n["n1"]
    g["start"] = "n2"


def remove_ahead(n, g):
    del n["n3"]
    n["n2"]["next"]["received"] = "n4"


def remove_current(n, g):
    del n["n2"]
    n["n1"]["next"]["ok"] = "n3"


def reorder_future(n, g):
    n["n2"]["next"]["received"] = "n4"
    n["n4"]["next"]["ok"] = "n3"
    n["n3"]["next"]["ok"] = None


def reconfigure_passed(n, g):
    n["n1"]["config"]["template"] = "welcome-v2"


def reconfigure_current(n, g):
    n["n2"]["config"]["name"] = "go2"


def retype_passed(n, g):
    n["n1"] = timer(60, "n2")


# name: (change, actions after publish, expected side effects in order)
SCENARIOS = {
    "add_node_ahead": (add_ahead, [("signal", "go")], ["n1", "nX", "n3", "n4"]),
    "add_node_behind": (add_behind, [("signal", "go")], ["n1", "n3", "n4"]),
    "remove_passed_node": (remove_passed, [("signal", "go")], ["n1", "n3", "n4"]),
    "remove_node_ahead": (remove_ahead, [("signal", "go")], ["n1", "n4"]),
    "remove_current_node": (remove_current, [("nudge", None)], ["n1", "n3", "n4"]),
    "reorder_future_nodes": (reorder_future, [("signal", "go")], ["n1", "n4", "n3"]),
    "reconfigure_passed_node": (reconfigure_passed, [("signal", "go")], ["n1", "n3", "n4"]),
    "reconfigure_current_node": (reconfigure_current, [("nudge", None), ("signal", "go2")], ["n1", "n3", "n4"]),
    "retype_passed_node": (retype_passed, [("signal", "go")], ["n1", "n3", "n4"]),
}


@pytest.mark.parametrize("name", list(SCENARIOS))
async def test_graph_change_mid_run_is_replay_safe(ts, name):
    fn, actions, expected = SCENARIOS[name]
    ts.store.publish(v1())
    async with ts.worker(max_cached_workflows=0):
        handle = await ts.start(name)
        await wait_at(handle, "n2")
        parked = await handle.fetch_history()

        ts.store.publish(change(fn)())
        await assert_replays(parked)  # old history + new graph: replay uses the recorded fetch results

        for kind, arg in actions:
            if kind == "nudge":
                await handle.signal("nudge")
            else:
                await send(handle, arg)
        await handle.result()

    final = await handle.fetch_history()
    await assert_replays(final)
    assert rebuilds(ts.jid(name)) > 0  # the live run really was rebuilt from history after the publish
    assert ts.effects(name) == expected  # followed v2 from its position, n1 ran exactly once
    if name == "reconfigure_passed_node":
        assert ts.ledger.steps(ts.jid(name))[0]["config"]["template"] == "welcome"
    save_history(f"q2_{name}", final)


async def test_remove_timer_node_a_journey_is_waiting_on(ts):
    ts.store.publish(graph("t", t=timer(3 * 24 * 3600, "n2"), n2=act(None)))
    async with ts.worker():
        handle = await ts.start("timer-removed")
        await wait_at(handle, "t")
        parked = await handle.fetch_history()
        ts.store.publish(graph("n2", n2=act(None)))  # the timer node is removed while the journey waits on it
        await assert_replays(parked)
        await handle.signal("nudge")
        await handle.result()

    final = await handle.fetch_history()
    assert "TIMER_FIRED" not in event_types(final)  # the wait was abandoned, not sat out
    assert ts.effects("timer-removed") == ["n2"]
    await assert_replays(final)
