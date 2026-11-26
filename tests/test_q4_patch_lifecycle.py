"""Q4. Interpreter code change behind workflow.patched(): patch -> deprecate -> remove.

The change: an `audit` activity is added before every fetch_next_node (a loop change, see Q3).
Code versions:
  v1             original interpreter
  v2-unpatched   change shipped without a patch (NOT a lifecycle step: the mistake)
  v2-patched     if workflow.patched("audit-before-fetch"): audit
  v3-deprecated  workflow.deprecate_patch("audit-before-fetch"); audit
  v4-removed     audit, no patch call (same code as v2-unpatched)
Histories: one journey parked mid-run, recorded by v1, by v2-patched and by v3-deprecated.
"""

import pytest

from dsl.workflow import GraphInterpreterWorkflow
from temporalio.client import WorkflowExecutionStatus

from tests.helpers import act, graph, is_nde, replay, send, sig, wait_at, wait_until, workflow_task_failures
from variants.interpreters import ExtraActivityDeprecated, ExtraActivityPatched, ExtraActivityUnpatched

CODE = {
    "v1": GraphInterpreterWorkflow,
    "v2-unpatched": ExtraActivityUnpatched,
    "v2-patched": ExtraActivityPatched,
    "v3-deprecated": ExtraActivityDeprecated,
    "v4-removed": ExtraActivityUnpatched,
}
RECORDED_BY = {"h-v1": GraphInterpreterWorkflow, "h-v2": ExtraActivityPatched, "h-v3": ExtraActivityDeprecated}

# Observed grid (OK = replays, NDE = non-determinism error)
EXPECTED = {
    #                 h-v1    h-v2    h-v3
    "v1":            ("OK",  "NDE", "NDE"),  # rolling deploy: a new-branch history on an old worker
    "v2-unpatched":  ("NDE", "NDE", "OK"),
    "v2-patched":    ("OK",  "OK",  "OK"),
    "v3-deprecated": ("NDE", "OK",  "OK"),
    "v4-removed":    ("NDE", "NDE", "OK"),   # a history with a non-deprecated patch marker needs the call
}


@pytest.fixture
async def histories(ts):
    ts.store.publish(graph("n1", n1=act("n2"), n2=sig("go", "n3"), n3=act(None)))
    recorded = {}
    for name, cls in RECORDED_BY.items():
        async with ts.worker(cls):
            handle = await ts.start(name)
            await wait_at(handle, "n2")
            recorded[name] = await handle.fetch_history()
    return recorded


async def test_patch_lifecycle_grid(histories):
    grid = {}
    for code, cls in CODE.items():
        grid[code] = tuple(["NDE" if is_nde(await replay(h, cls)) else "OK" for h in histories.values()])

    print("\ncode \\ history   " + "  ".join(f"{h:>5}" for h in histories))
    for code, row in grid.items():
        print(f"{code:<16} " + "  ".join(f"{c:>5}" for c in row))
    assert grid == EXPECTED


async def test_lifecycle_steps_are_safe_only_when_drained(histories):
    """Each step is safe exactly when no run from before the previous step is still open."""
    assert await replay(histories["h-v1"], ExtraActivityPatched) is None  # step 1: patch keeps v1 runs working
    assert is_nde(await replay(histories["h-v1"], ExtraActivityDeprecated))  # step 2 too early: v1 run still open
    assert await replay(histories["h-v2"], ExtraActivityDeprecated) is None  # step 2 after v1 runs drained
    assert is_nde(await replay(histories["h-v2"], ExtraActivityUnpatched))  # skipping step 2 breaks v2 runs
    assert await replay(histories["h-v3"], ExtraActivityUnpatched) is None  # step 3: remove the call


async def test_rolling_deploy_old_worker_gets_new_branch_history_then_heals(local):
    """Live: a journey that took the patched branch lands on an old worker, sticks, and heals on a new one."""
    local.store.publish(graph("n1", n1=act("n2"), n2=sig("go", "n3"), n3=act(None)))
    async with local.worker(ExtraActivityPatched):  # new worker: takes the new branch, records the marker
        handle = await local.start("rolling")
        await wait_at(handle, "n2")
        attributes = (await handle.describe()).search_attributes

    async with local.worker(GraphInterpreterWorkflow):  # only an old worker is polling
        await send(handle, "go")
        failures = await wait_until(lambda: _failures(handle), timeout=30)
        assert any("Nondeterminism" in f for f in failures)
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING  # stuck, not failed
    assert local.effects("rolling") == ["n1"]

    async with local.worker(ExtraActivityPatched):  # the rollout finishes: a new worker picks it up
        await handle.result()
    assert local.effects("rolling") == ["n1", "n3"]
    # how to tell which runs carry the patch marker (and so which still need the old branch)
    assert attributes["TemporalChangeVersion"] == ["audit-before-fetch"]


async def _failures(handle):
    return workflow_task_failures(await handle.fetch_history())
