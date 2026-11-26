"""The integration surface: a client plugs in its own handler and drives journeys only through dsl.client."""

from dsl import client as journeys
from reference.json_store import JsonGraphStore, JsonLedger
from dsl.ports import NodeCall
from dsl.worker import create_worker
from tests.helpers import act, graph, sig, wait_until


async def test_client_supplies_its_own_handler_and_uses_the_client_api(local_env, tmp_path):
    graphs, ledger = JsonGraphStore(tmp_path), JsonLedger(tmp_path)
    graphs.publish(graph("n1", n1=act("s", kind="crm_update", field="stage"), s=sig("signed", "n3"), n3=act(None)))
    calls = []

    async def crm_update(call: NodeCall) -> str:  # the client's own business kind
        calls.append(call.idempotency_key)
        return "ok"

    async def noop(call: NodeCall) -> str:
        return "ok"

    handlers = {"crm_update": crm_update, "noop": noop}
    client, queue, journey = local_env.client, "integration", "journey-42"
    async with create_worker(client, graphs, ledger, handlers, task_queue=queue):
        handle = await journeys.start_journey(client, journey, "g", queue)
        await wait_until(lambda: _at(client, journey, "s"))
        await journeys.send_event(client, journey, "signed")
        await handle.result()

    assert calls == [f"{journey}:1:n1"]
    assert [s["node_id"] for s in ledger.steps(journey)] == ["n1", "n3"]


async def _at(client, journey, node):
    return (await journeys.state(client, journey))["current_node"] == node
