"""Replay safety is not business safety. These tests cover the gap between the two.

- A graph published between fetching a node and executing it: which configuration runs?
- A crash after the external side effect succeeded but before the activity completed: does it repeat?
"""

import pytest
from temporalio import activity

from reference.handlers import FakeProvider
from dsl.models import FetchInput, NodeSpec
from tests.helpers import act, assert_replays, graph, scheduled_activities


def v1():
    return graph("n1", n1=act("n2", kind="send_email", template="welcome"), n2=act(None))


def v2_reconfigured():
    return graph("n1", n1=act("n2", kind="send_email", template="welcome-v2"), n2=act(None))


def v2_removed():
    return graph("n2", n2=act(None))


@pytest.mark.parametrize(
    "v2, template, version_used",
    [(v2_reconfigured, "welcome-v2", 2), (v2_removed, "welcome", 1)],
    ids=["reconfigured", "removed"],
)
async def test_graph_published_between_fetch_and_execute(ts, v2, template, version_used):
    ts.store.publish(v1())
    fetched = []

    @activity.defn(name="fetch_next_node")
    async def fetch_then_publish(inp: FetchInput) -> NodeSpec:
        spec = await ts.dsl.fetch_next_node(inp)
        if spec.node_id == "n1" and not fetched:
            fetched.append(spec)
            ts.store.publish(v2())  # lands after n1 was fetched, before it is executed
        return spec

    activities = [a for a in ts.dsl.all() if a.__name__ != "fetch_next_node"] + [fetch_then_publish] + ts.extra.all()
    async with ts.worker(activities=activities):
        handle = await ts.start("between")
        await handle.result()

    assert fetched[0].graph_version == 1 and fetched[0].config["template"] == "welcome"
    ran = ts.ledger.steps(ts.jid("between"))[0]
    # reconfigured: the latest config runs. removed: the node runs once, as it was fetched.
    assert (ran["node_id"], ran["config"]["template"], ran["graph_version"]) == ("n1", template, version_used)
    assert ts.effects("between") == ["n1", "n2"]
    await assert_replays(await handle.fetch_history())


class CrashAfterEffect(FakeProvider):
    """The external call succeeds, then the worker dies before the activity can record it."""

    def __init__(self, honor_idempotency_key: bool) -> None:
        super().__init__(honor_idempotency_key)
        self.crashed = False

    def deliver(self, idempotency_key, payload):
        super().deliver(idempotency_key, payload)
        if not self.crashed:
            self.crashed = True
            raise RuntimeError("worker crashed after the side effect, before the ledger write")


@pytest.mark.parametrize("honor_key, expected_effects", [(False, 2), (True, 1)], ids=["no-key", "idempotency-key"])
async def test_crash_after_side_effect_before_completion(ts, honor_key, expected_effects):
    ts.store.publish(graph("n1", n1=act(None, kind="send_email", template="welcome")))
    ts.handlers.provider = CrashAfterEffect(honor_key)
    async with ts.worker():
        handle = await ts.start("crash")
        await handle.result()

    history = await handle.fetch_history()
    assert scheduled_activities(history).count("execute_node") == 1  # one activity, retried by Temporal
    assert len(ts.handlers.provider.calls) == 2 and len(set(ts.handlers.provider.calls)) == 1  # same key both times
    assert ts.effects("crash") == ["n1"]  # the ledger shows one run either way: it cannot see the repeat
    assert len(ts.handlers.provider.effects) == expected_effects  # only the idempotency key prevents the duplicate
    await assert_replays(history)
