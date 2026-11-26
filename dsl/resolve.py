"""Next-node resolution: follow the latest graph from the current position, never rerun a node.

Pure function of its inputs. Called by the fetch_next_node activity (safe), and by the
unsafe test variants that read the graph inside workflow code (to reproduce NDEs).
"""

from typing import Any, Dict, Optional

from dsl.models import NodeSpec, NodeStatus, Position

END = NodeSpec(status=NodeStatus.END)
ORPHANED = NodeSpec(status=NodeStatus.ORPHANED)


def _spec(graph: Dict[str, Any], node_id: str) -> NodeSpec:
    node = graph["nodes"][node_id]
    return NodeSpec(
        status=NodeStatus.NODE,
        node_id=node_id,
        type=node["type"],
        kind=node.get("kind"),
        config=dict(node.get("config", {})),
        graph_version=graph["version"],
    )


def _first_exit(node: Dict[str, Any]) -> Optional[str]:
    nxt = node.get("next", {})
    return next(iter(nxt.values()), None)


def resolve_next(
    latest: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
    pos: Position,
    completed: Dict[str, str],
    goto: Optional[str] = None,
) -> NodeSpec:
    nodes = latest["nodes"]

    # Rule 2: a pending goto wins.
    if goto is not None:
        return _spec(latest, goto) if goto in nodes else ORPHANED

    # Rule 1: no position yet.
    if pos.node_id is None:
        candidate: Optional[str] = latest["start"]
    elif pos.node_id in nodes:
        # Node still in progress (a chunked wait): re-enter it with its latest config.
        if pos.last_outcome is None:
            return _spec(latest, pos.node_id)
        # Rule 3: follow the latest edges.
        candidate = nodes[pos.node_id].get("next", {}).get(pos.last_outcome)
    else:
        # Rule 4: current node was removed; walk the version the journey was on.
        if previous is None or pos.node_id not in previous["nodes"]:
            return ORPHANED
        old = previous["nodes"]
        cur = old[pos.node_id]
        candidate = cur.get("next", {}).get(pos.last_outcome) if pos.last_outcome else _first_exit(cur)
        seen = set()
        while candidate is not None and candidate not in nodes:
            if candidate in seen or candidate not in old:
                return ORPHANED
            seen.add(candidate)
            candidate = _first_exit(old[candidate])

    # Rule 5: skip nodes already completed by this journey.
    seen = set()
    while candidate is not None and candidate in completed:
        if candidate in seen or candidate not in nodes:
            return ORPHANED
        seen.add(candidate)
        candidate = nodes[candidate].get("next", {}).get(completed[candidate])

    if candidate is None:
        return END
    # Rule 6: nothing sensible to run.
    if candidate not in nodes:
        return ORPHANED
    return _spec(latest, candidate)
