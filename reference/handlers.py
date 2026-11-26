"""Reference node handlers (stand-in). Replace the fake provider with real clients (email service, CRM, ...).

Adding or changing a business kind changes only this file, never the workflow, so it needs no patch.
"""

import asyncio
from typing import Any, Dict, List, Optional

from dsl.ports import NodeCall, NodeHandler, config_error


def require(call: NodeCall, key: str) -> Any:
    value = call.config.get(key)
    if not value:
        raise config_error(f"node {call.node_id}: missing config '{key}'")
    return value


class FakeProvider:
    """Stand-in for an external system that de-duplicates on an idempotency key."""

    def __init__(self, honor_idempotency_key: bool = True) -> None:
        self.honor_idempotency_key = honor_idempotency_key
        self.calls: List[str] = []  # every request received, by idempotency key
        self.effects: List[str] = []  # what actually happened (emails sent, records changed)

    def deliver(self, idempotency_key: str, payload: Dict[str, Any]) -> None:
        self.calls.append(idempotency_key)
        if self.honor_idempotency_key and idempotency_key in self.calls[:-1]:
            return
        self.effects.append(idempotency_key)


class ExampleHandlers:
    def __init__(self, provider: Optional[FakeProvider] = None) -> None:
        self.provider = provider or FakeProvider()

    async def _call_provider(self, call: NodeCall, *required: str) -> str:
        payload = {key: require(call, key) for key in required}
        await asyncio.sleep(float(call.config.get("delay_seconds", 0)))  # simulates a slow external call
        if required:
            self.provider.deliver(call.idempotency_key, payload)
        return "ok"

    async def send_email(self, call: NodeCall) -> str:
        return await self._call_provider(call, "template")

    async def update_record(self, call: NodeCall) -> str:
        return await self._call_provider(call, "field")

    async def noop(self, call: NodeCall) -> str:
        return await self._call_provider(call)

    def as_dict(self) -> Dict[str, NodeHandler]:
        return {"send_email": self.send_email, "update_record": self.update_record, "noop": self.noop}
