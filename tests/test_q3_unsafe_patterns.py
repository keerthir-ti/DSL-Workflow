"""Q3. Which implementation choices break replay? (More interpreter code changes and their patched
fixes are in test_more_nde_scenarios.py.)

Each test records a history, makes one change, and replays the old history: NDE (or a silent wrong
result) for the unsafe pattern, a clean replay for the safe design or the patched fix.
"""

import copy
import importlib
import json
import os

import pytest

from dsl.workflow import GraphInterpreterWorkflow
from tests.helpers import (
    act,
    assert_nde,
    assert_replays,
    graph,
    park,
    replay,
    scheduled_activities,
    send,
    sig,
    timer,
    wait_at,
    wait_until,
    workflow_task_failures,
)
from variants import shared_state
from variants.interpreters import (
    ClientFlagInCode,
    ControlActivityArgs,
    ControlActivityOptions,
    ControlTimerDuration,
    ExtraActivityUnpatched,
    GraphInGlobal,
    GraphInProcessCache,
    TimerAsActivity,
    TimerAsActivityPatched,
    UuidOneCall,
    UuidTwoCalls,
)


def v1():
    return graph("n1", n1=act("n2", kind="send_email", template="welcome"), n2=sig("go", "n3"), n3=act(None))


def v2_retyped():
    g = copy.deepcopy(v1())
    g["nodes"]["n1"] = timer(60, "n2")
    return g


def v2_inserted():
    g = copy.deepcopy(v1())
    g["nodes"]["n0"] = act("n1")
    g["start"] = "n0"
    return g


def versioned(g, version):
    return {**g, "version": version}


# ------------------------------------------------------------------ graph read in workflow code
async def test_graph_in_global_nde_on_retype(ts):
    ts.store.publish(v1())
    shared_state.GRAPH["latest"] = versioned(v1(), 1)
    _, history = await park(ts, "global", workflow_cls=GraphInGlobal)
    shared_state.GRAPH["latest"] = versioned(v2_retyped(), 2)  # user publishes v2
    await assert_nde(history, GraphInGlobal)

    # Fix: the safe interpreter, same change -> replay passes
    _, safe_history = await park(ts, "global-safe", workflow_cls=GraphInterpreterWorkflow)
    ts.store.publish(v2_retyped())
    await assert_replays(safe_history)


async def test_graph_in_global_silent_rerun_on_insert(ts):
    """An insert before the position gives the same command types, so no NDE: the journey re-runs n1."""
    ts.store.publish(v1())
    shared_state.GRAPH["latest"] = versioned(v1(), 1)
    handle, history = await park(ts, "global-insert", workflow_cls=GraphInGlobal)
    shared_state.GRAPH["latest"] = versioned(v2_inserted(), 2)
    assert await replay(history, GraphInGlobal) is None  # replay "passes" ...
    async with ts.worker(GraphInGlobal, max_cached_workflows=0):
        await send(handle, "go")
        await handle.result()
    assert ts.effects("global-insert") == ["n1", "n1", "n3"]  # ... but n1 ran twice (silent corruption)


async def test_graph_file_loaded_on_import_blocked_by_sandbox(ts, tmp_path):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(versioned(v1(), 1)))
    os.environ["DSL_GRAPH_FILE"] = str(path)
    from variants.graph_file_sandboxed import GraphFileSandboxed

    with pytest.raises(RuntimeError, match="Failed validating workflow") as info:
        ts.worker(GraphFileSandboxed)
    assert "RestrictedWorkflowAccessError" in repr(info.value.__cause__)


async def test_graph_file_loaded_on_import_passthrough_nde(ts, tmp_path):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(versioned(v1(), 1)))
    os.environ["DSL_GRAPH_FILE"] = str(path)
    from variants import graph_file_loader  # the worker process imports its "config" module at start-up

    importlib.reload(graph_file_loader)
    from variants.graph_file_passthrough import GraphFilePassThrough

    ts.store.publish(v1())
    _, history = await park(ts, "file", workflow_cls=GraphFilePassThrough)
    path.write_text(json.dumps(versioned(v2_retyped(), 2)))  # publish v2 to the file
    importlib.reload(graph_file_loader)  # worker restart re-imports the module
    await assert_nde(history, GraphFilePassThrough)


async def test_graph_in_process_cache_nde(local):
    shared_state.CACHE.clear()
    local.store.publish(v1())
    async with local.worker(GraphInProcessCache):  # workflow cache on: works while nothing is evicted
        first = await local.start("cache-a")
        await wait_at(first, "n2")
        second = await local.start("cache-b")
        await wait_at(second, "n2")
        history_a, history_b = await first.fetch_history(), await second.fetch_history()

        # the cache serves a stale graph: v2 (new start node n0) is published, a new journey still walks v1
        local.store.publish(v2_inserted())
        stale = await local.start("cache-stale")
        await wait_at(stale, "n2")
        assert local.effects("cache-stale") == ["n1"]  # n0 from the latest version was never run
        for handle in (first, second, stale):
            await handle.terminate()

    assert "get_graph" in scheduled_activities(history_a)
    assert "get_graph" not in scheduled_activities(history_b)  # cache hit: no recorded fetch
    await assert_nde(history_a, GraphInProcessCache)  # same process, cache warm -> skips get_graph
    shared_state.CACHE.clear()  # another worker / a restart -> cache cold
    await assert_nde(history_b, GraphInProcessCache)  # now schedules get_graph where history has none


async def test_graph_in_process_cache_breaks_on_eviction(local):
    """With the workflow cache off (= every eviction), the journey NDEs on its own second task."""
    shared_state.CACHE.clear()
    local.store.publish(v1())
    async with local.worker(GraphInProcessCache, max_cached_workflows=0):
        handle = await local.start("cache-evict")
        failures = await wait_until(lambda: _failures(handle))
        await handle.terminate()
    assert any("Nondeterminism" in f for f in failures)


async def _failures(handle):
    return workflow_task_failures(await handle.fetch_history())


# ------------------------------------------------------------ node type -> command mapping changes
async def test_timer_mapped_to_activity_nde_and_patch_fix(ts):
    ts.store.publish(graph("t1", t1=timer(3 * 24 * 3600, "n2"), n2=sig("go", None)))
    async with ts.worker():
        handle = await ts.start("timer-map")
        await ts.env.sleep(3 * 24 * 3600 + 60)
        await wait_at(handle, "n2")
        history = await handle.fetch_history()
    await assert_nde(history, TimerAsActivity)
    await assert_replays(history, TimerAsActivityPatched)  # fix: old histories keep the timer branch

    _, new_history = await park(ts, "timer-map-new", workflow_cls=TimerAsActivityPatched)
    assert "durable_wait" in scheduled_activities(new_history)  # new journeys take the new branch


async def test_interpreter_loop_extra_activity_nde(ts):
    ts.store.publish(v1())
    _, history = await park(ts, "loop", workflow_cls=GraphInterpreterWorkflow)
    message = await assert_nde(history, ExtraActivityUnpatched)
    assert "fetch_next_node" in message and "audit" in message


async def test_per_client_flag_in_workflow_code_nde(ts):
    ts.store.publish(v1())
    shared_state.CLIENT_FLAG["extra_step"] = False
    _, history = await park(ts, "flag", workflow_cls=ClientFlagInCode)
    shared_state.CLIENT_FLAG["extra_step"] = True  # the client is switched to the new behaviour
    try:
        await assert_nde(history, ClientFlagInCode)  # fix: test_more_nde_scenarios client_flag_read_through_an_activity
    finally:
        shared_state.CLIENT_FLAG["extra_step"] = False


# ------------------------------------------------------------------ silent drift and control group
async def test_uuid4_call_order_changes_ids_without_nde(ts):
    ts.store.publish(v1())
    shared_state.SEEN_IDS.clear()
    _, history = await park(ts, "uuid", workflow_cls=UuidOneCall)
    live_ids = set(shared_state.SEEN_IDS)

    shared_state.SEEN_IDS.clear()
    await assert_replays(history, UuidOneCall)
    assert set(shared_state.SEEN_IDS) == live_ids  # deterministic on replay

    shared_state.SEEN_IDS.clear()
    await assert_replays(history, UuidTwoCalls)  # no NDE ...
    assert set(shared_state.SEEN_IDS).isdisjoint(live_ids)  # ... but every id is now different


@pytest.mark.parametrize("variant", [ControlActivityOptions, ControlActivityArgs, ControlTimerDuration])
async def test_changes_not_checked_by_replay(ts, variant):
    """Timeouts, retry policy, activity arguments and timer duration are not compared on replay."""
    ts.store.publish(graph("n1", n1=act("t1"), t1=timer(60, "n2"), n2=sig("go", None)))
    async with ts.worker():
        handle = await ts.start(f"control-{variant.__name__}")
        await ts.env.sleep(120)
        await wait_at(handle, "n2")
        history = await handle.fetch_history()
    await assert_replays(history, variant)
