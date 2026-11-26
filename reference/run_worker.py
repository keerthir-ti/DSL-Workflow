"""Run a worker on the stand-ins: python -m reference.run_worker (JSON files in ./data, local Temporal)."""

import asyncio
import os
from pathlib import Path

from temporalio.client import Client

from dsl.worker import DEFAULT_TASK_QUEUE, create_worker
from reference.handlers import ExampleHandlers
from reference.json_store import JsonGraphStore, JsonLedger


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    data = Path(os.environ.get("DSL_DATA_DIR", "data"))
    worker = create_worker(
        client, JsonGraphStore(data), JsonLedger(data), ExampleHandlers().as_dict(),
        os.environ.get("TEMPORAL_TASK_QUEUE", DEFAULT_TASK_QUEUE),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
