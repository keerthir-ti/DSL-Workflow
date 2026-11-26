"""GraphRepository and JourneyLedger on JSON files (stand-in).

Replace both with your database. The interpreter only depends on the interfaces in dsl/ports.py.
"""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from dsl.models import NODE_TYPES, Position
from dsl.ports import StepRecord


class GraphValidationError(ValueError):
    pass


def validate(graph: Dict[str, Any]) -> None:
    """Checks run at publish time, so a bad graph never reaches a running journey."""
    nodes = graph.get("nodes", {})
    if graph.get("start") not in nodes:
        raise GraphValidationError("start node missing")
    for node_id, node in nodes.items():
        if node.get("type") not in NODE_TYPES:
            raise GraphValidationError(f"{node_id}: unknown type {node.get('type')!r}")
        for outcome, target in node.get("next", {}).items():
            if target is not None and target not in nodes:
                raise GraphValidationError(f"{node_id}.next[{outcome}] -> unknown node {target!r}")


class JsonGraphStore:
    """graphs/<graph_id>/v<N>.json (immutable) + graphs/<graph_id>/latest.json (pointer)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root) / "graphs"

    def _dir(self, graph_id: str) -> Path:
        return self.root / graph_id

    def latest_version(self, graph_id: str) -> int:
        pointer = self._dir(graph_id) / "latest.json"
        return json.loads(pointer.read_text())["version"] if pointer.exists() else 0

    def publish(self, graph: Dict[str, Any]) -> int:
        """Editor side: validate and store a new immutable version, then move the latest pointer."""
        validate(graph)
        graph_id = graph["graph_id"]
        version = self.latest_version(graph_id) + 1
        directory = self._dir(graph_id)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"v{version}.json").write_text(json.dumps({**graph, "version": version}, indent=2))
        (directory / "latest.json").write_text(json.dumps({"version": version}))
        return version

    def get(self, graph_id: str, version: int) -> Dict[str, Any]:
        return json.loads((self._dir(graph_id) / f"v{version}.json").read_text())

    def latest(self, graph_id: str) -> Dict[str, Any]:
        return self.get(graph_id, self.latest_version(graph_id))


class JsonLedger:
    """journeys/<journey_id>.json: position, completed nodes and completed steps."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root) / "journeys"
        self.root.mkdir(parents=True, exist_ok=True)

    def _file(self, journey_id: str) -> Path:
        return self.root / f"{journey_id.replace(':', '__')}.json"

    def _load(self, journey_id: str) -> Dict[str, Any]:
        path = self._file(journey_id)
        return json.loads(path.read_text()) if path.exists() else {"position": None, "completed": {}, "steps": []}

    def _save(self, journey_id: str, data: Dict[str, Any]) -> None:
        self._file(journey_id).write_text(json.dumps(data, indent=2))

    def position(self, journey_id: str) -> Optional[Position]:
        stored = self._load(journey_id)["position"]
        return Position(**stored) if stored else None

    def commit_position(self, journey_id: str, position: Position) -> Mapping[str, str]:
        data = self._load(journey_id)
        if position.node_id and position.last_outcome:
            data["completed"][position.node_id] = position.last_outcome
        data["position"] = asdict(position)
        self._save(journey_id, data)
        return data["completed"]

    def completed_nodes(self, journey_id: str) -> Mapping[str, str]:
        return self._load(journey_id)["completed"]

    def step_outcome(self, journey_id: str, step_no: int) -> Optional[str]:
        return next((s["outcome"] for s in self._load(journey_id)["steps"] if s["step_no"] == step_no), None)

    def record_step(self, journey_id: str, step: StepRecord) -> None:
        data = self._load(journey_id)
        data["steps"].append(asdict(step))
        data["completed"][step.node_id] = step.outcome
        self._save(journey_id, data)

    def steps(self, journey_id: str) -> List[Dict[str, Any]]:
        """Completed steps in the order they ran (for inspection and tests)."""
        return self._load(journey_id)["steps"]
