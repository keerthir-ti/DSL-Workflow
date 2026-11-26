import os
import shutil
from pathlib import Path

import pytest
import pytest_asyncio
from temporalio.common import SearchAttributeKey
from temporalio.testing import WorkflowEnvironment

from tests.helpers import Ctx


def pytest_collection_modifyitems(items):
    marker = pytest.mark.asyncio(loop_scope="session")
    for item in items:
        if pytest_asyncio.is_async_test(item):
            item.add_marker(marker, append=False)


def _temporal_cli() -> dict:
    path = os.environ.get("TEMPORAL_CLI") or shutil.which("temporal")
    fallback = Path.home() / ".local/bin/temporal"
    if not path and fallback.exists():
        path = str(fallback)
    return {"dev_server_existing_path": path} if path else {}


@pytest_asyncio.fixture(scope="session")
async def ts_env():
    """Time-skipping test server: timers of days complete instantly."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@pytest_asyncio.fixture(scope="session")
async def local_env():
    """Local dev server: full server features (reset, update, visibility)."""
    node_attribute = SearchAttributeKey.for_keyword("DslNode")  # used by the search-attribute scenario
    async with await WorkflowEnvironment.start_local(search_attributes=[node_attribute], **_temporal_cli()) as env:
        yield env


@pytest.fixture
def ts(ts_env, tmp_path) -> Ctx:
    return Ctx(ts_env, tmp_path)


@pytest.fixture
def local(local_env, tmp_path) -> Ctx:
    return Ctx(local_env, tmp_path)
