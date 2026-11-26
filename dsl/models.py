"""Data passed between the interpreter workflow and its activities.

Everything here is a plain dataclass so it is safe to import inside the workflow sandbox.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

WORKFLOW_NAME = "GraphInterpreter"
NODE_TYPES = ("activity", "timer", "signal", "fanout")
CONFIG_ERROR = "ConfigError"  # ApplicationError type of a configuration problem: parks the journey


class NodeStatus:
    """NodeSpec.status values."""

    NODE, END, ORPHANED = "node", "end", "orphaned"


class GotoMode:
    """How an accepted goto is applied."""

    IN_PLACE, CONTINUE_AS_NEW = "in_place", "continue_as_new"


@dataclass
class Position:
    """Where a journey stands. Committed to the ledger by fetch_next_node."""

    node_id: Optional[str] = None
    step_no: int = 0
    last_outcome: Optional[str] = None  # None = node at node_id is still in progress (a wait)
    graph_version: Optional[int] = None
    wait_started_at: Optional[float] = None  # epoch seconds, set while a timer node is in progress


@dataclass
class NodeSpec:
    """Complete decision returned by fetch_next_node. The workflow needs nothing else."""

    status: str  # NodeStatus
    node_id: Optional[str] = None
    type: Optional[str] = None
    kind: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    graph_version: Optional[int] = None


@dataclass
class JourneyInput:
    journey_id: str
    graph_id: str
    hops_per_run: int = 0  # continue-as-new after this many nodes in one run (0 = off)
    timer_chunk_seconds: int = 0  # longest single timer; longer waits continue-as-new (0 = off)
    start_node: Optional[str] = None  # goto applied by the first fetch of the run
    pending_events: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class FetchInput:
    journey_id: str
    graph_id: str
    position: Position
    goto: Optional[str] = None


@dataclass
class ExecuteInput:
    journey_id: str
    graph_id: str
    step_no: int
    node_id: str
    graph_version: Optional[int] = None


@dataclass
class GotoInput:
    journey_id: str
    graph_id: str
    target: str


@dataclass
class GotoRequest:
    target: str
    mode: str = GotoMode.IN_PLACE


@dataclass
class Event:
    name: str
    payload: Any = None


@dataclass
class ParticipantInput:
    journey_id: str
    participant: str
    graph_id: str
