"""Replay gate: every captured history in histories/ must replay against the current interpreter.

Run this before every deploy (CI). Refresh the captured histories with UPDATE_HISTORIES=1 pytest.
In production the same test runs against histories sampled from running journeys.
"""

import json

import pytest
from temporalio.client import WorkflowHistory

from dsl.workflow import GraphInterpreterWorkflow
from tests.helpers import HISTORIES, assert_replays

FILES = sorted(HISTORIES.glob("*.json"))


@pytest.mark.skipif(not FILES, reason="no captured histories; run UPDATE_HISTORIES=1 pytest")
@pytest.mark.parametrize("path", FILES, ids=[p.stem for p in FILES])
async def test_captured_history_replays_on_current_code(path):
    doc = json.loads(path.read_text())
    history = WorkflowHistory.from_json(doc["workflow_id"], doc["history"])
    await assert_replays(history, GraphInterpreterWorkflow)
