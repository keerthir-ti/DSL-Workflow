"""The three things a client implements to run the interpreter on its own systems.

The interpreter workflow and its activities depend only on these interfaces. `reference/` holds
stand-in implementations (JSON files, a fake provider) to replace.
"""

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Protocol

from temporalio.exceptions import ApplicationError

from dsl.models import CONFIG_ERROR, Position


class GraphRepository(Protocol):
    """Published graphs. A published version never changes; old versions stay readable."""

    def latest(self, graph_id: str) -> Dict[str, Any]:
        """The latest published version: {"graph_id", "version", "start", "nodes": {...}}."""

    def get(self, graph_id: str, version: int) -> Dict[str, Any]:
        """A specific published version (used to walk past nodes removed since the journey's version)."""


@dataclass
class StepRecord:
    """One completed node execution: the business record of what ran."""

    step_no: int
    node_id: str
    kind: str
    outcome: str
    graph_version: int
    config: Dict[str, Any] = field(default_factory=dict)


class JourneyLedger(Protocol):
    """Where each journey stands and what it already ran. Lives outside Temporal (e.g. a database table).

    In production each method is one transaction.
    """

    def position(self, journey_id: str) -> Optional[Position]:
        """The last committed position, or None for a new journey."""

    def commit_position(self, journey_id: str, position: Position) -> Mapping[str, str]:
        """Store the position. If it has an outcome, mark its node completed. Return completed node -> outcome."""

    def completed_nodes(self, journey_id: str) -> Mapping[str, str]:
        """Completed node -> outcome, for this journey."""

    def step_outcome(self, journey_id: str, step_no: int) -> Optional[str]:
        """Outcome of a step that already completed, or None."""

    def record_step(self, journey_id: str, step: StepRecord) -> None:
        """Mark the step and its node completed."""


@dataclass
class NodeCall:
    """What a node handler receives."""

    journey_id: str
    step_no: int
    node_id: str
    kind: str
    config: Dict[str, Any]
    graph_version: int
    idempotency_key: str  # identical on every retry of this step: send it to the external system


# A business node kind (send_email, update_record, ...). Returns the outcome used to pick the next edge.
# Raise config_error(...) for a configuration problem: the journey parks until resumed.
NodeHandler = Callable[[NodeCall], Awaitable[str]]


def config_error(message: str) -> ApplicationError:
    """A configuration problem: not retried; the journey parks as NEEDS_ATTENTION until resumed."""
    return ApplicationError(message, type=CONFIG_ERROR, non_retryable=True)
