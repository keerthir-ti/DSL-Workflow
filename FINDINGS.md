# Findings: replay safety of a graph-driven (DSL) Temporal workflow

Every statement below is backed by a passing test in `tests/` (82 tests: Python 3.10, `temporalio` 1.35.0,
time-skipping test server and local dev server). The test name is given in brackets.

**Headline.** The starting hypothesis holds: when the next node is fetched through an activity, graph
changes never break replay. Replay breaks only when workflow code reads the graph some other way, or when
the interpreter code changes the commands it issues without a patch.

---

## 1. Change type × replay-safe × what to do

| Change | Replay-safe | What to do | Evidence |
|---|---|---|---|
| **Graph (published JSON)** | | | |
| Add a node ahead of or behind the journey | Yes | Publish. Journeys before it run it; journeys past it don't. | `q2[add_node_ahead]`, `q2[add_node_behind]` |
| Remove a node already passed, or one ahead | Yes | Publish. | `q2[remove_passed_node]`, `q2[remove_node_ahead]`, `q5 remove_node_from_graph` |
| Remove the node a journey is standing on | Yes | Publish, then nudge waiting journeys. A signal or timer wait is abandoned and the journey walks forward to the next node that still exists; an in-flight activity finishes first. | `q2[remove_current_node]`, `q2 remove_timer_node…`, `q5 remove_node_from_graph` |
| Reorder nodes ahead | Yes | Publish. | `q2[reorder_future_nodes]` |
| Reconfigure or change the type of a passed node | Yes | Publish. The old run keeps its recorded result; the node is not re-run. | `q2[reconfigure_passed_node]`, `q2[retype_passed_node]` |
| Reconfigure the node a journey is waiting on | Yes | Publish, then nudge. The wait re-reads its definition. | `q2[reconfigure_current_node]`, `q7 long_timer…adopts_reconfigured_wait` |
| **Business logic inside `execute_node`** | Yes | Deploy. Activity code is never replayed. | `q5 unregistered_activity…` |
| **Interpreter code** | | | |
| Add, remove or reorder an activity in the loop | **No** | `workflow.patched()` | `q3 interpreter_loop_extra_activity`, `q4` |
| Change what a node type maps to (timer → activity, child workflow → activity) | **No** | `workflow.patched()` | `q3 timer_mapped_to_activity…`, `more_nde fanout_child_to_activity` (patched fix tested) |
| Rename an activity type | **No** | `workflow.patched()`; keep the old name registered | `more_nde activity_renamed` (patched fix tested) |
| Merge `fetch_next_node` + `execute_node` into one call (cost) | **No** | `workflow.patched()`. Measured: 9 activity calls → 6 for a 4-node journey. | `q5 merge_fetch_and_execute…` |
| Activity timeout, retry policy, arguments; timer duration | Yes | Deploy. Replay does not compare them; old histories keep recorded results. | `q3 changes_not_checked_by_replay` |
| Change the continue-as-new condition in code | **No** | `workflow.patched()` | `more_nde continue_as_new_condition_changed` (patched fix tested) |
| Remove an activity from an update handler (e.g. `goto` stops validating) | **No** | `workflow.patched()` | `more_nde activity_removed_from_update_handler` (patched fix tested) |
| Change a signal handler that issues no commands | Yes | Deploy | `more_nde signal_handler_changed_without_commands_is_safe` |
| Start fan-out participants in a different order | **No** (child id mismatch) | `workflow.patched()` | `more_nde child_start_order_changed` (patched fix tested) |
| Add a search attribute upsert | **No** | `workflow.patched()` | `more_nde search_attribute_upsert_added` (patched fix tested) |
| Replay histories recorded by SDK 1.35.0 on SDK 1.33.0 | Yes (12/12 captured histories) | Run the replay gate on every SDK change | manual run, `histories/` |
| Number or order of `workflow.uuid4()` calls | Yes, **but ids silently change** | Treat as a command change: patch it. | `q3 uuid4_call_order…` |
| **Graph read in workflow code** | | | |
| Module global | **No** when a node type changes. **Worse** when a node is inserted: replay passes and an already-run node runs again. | Forbidden. | `q3 graph_in_global_nde_on_retype`, `q3 graph_in_global_silent_rerun_on_insert` |
| File loaded when the workflow module is imported | Worker refuses to start (sandbox blocks file access) | Forbidden. | `q3 graph_file…blocked_by_sandbox` |
| File loaded through a pass-through module | **No** | Forbidden. | `q3 graph_file…passthrough_nde` |
| Process-wide cache ("fetch only if not cached") | **No**. NDE on the journey's own next task once it is evicted, and stale graphs for new journeys. | Forbidden. A per-run cache held in workflow state is fine. | `q3 graph_in_process_cache_nde`, `…breaks_on_eviction` |
| Per-client flag read in workflow code | **No** | Deliver per-client behaviour as activity data. | `q3 per_client_flag…`; fix: `more_nde client_flag_read_through_an_activity_is_safe` |
| **Worker** | | | |
| Unregister an activity that is in flight | Yes (replay never runs activities) | The call retries with "not registered" until the activity is back. Keep it registered until nothing uses it. | `q5 unregistered_activity…` |

---

## 2. When replay happens

A run is "rebuilt" when a worker has to re-run the workflow code against the recorded history.
Measured with a probe that counts rebuilds (`test_q1_replay_triggers.py`).

| Trigger | Measured | Likely frequency in production |
|---|---|---|
| Cached worker, nothing else | 0 rebuilds over a whole journey | — |
| Worker restart, crash, rolling deploy | 1 full rebuild on the journey's next task | Every deploy empties the cache, so each running journey replays **on its next task** (not at deploy time). |
| Cache eviction (`max_cached_workflows=0`) | A rebuild on every workflow task | Routine once running journeys exceed the worker cache (SDK default 1000). A journey parked for days is evicted long before it wakes. |
| Query on an uncached workflow | +1 rebuild per query | Every status query on a parked journey |
| Query on a closed workflow | Always a rebuild | Every query after completion |
| Reset | New run rebuilds the history up to the reset point | Only when an operator resets |
| Continue-as-new | 0 rebuilds; the new run starts with an empty history | Never replays the old run |

Conclusion: for journeys that run for months, replay on newer code is the normal case, not the exception.
An NDE does not fail the workflow. It fails the workflow task, which retries: the journey stays `Running`
and stuck until compatible code is deployed (`q9 reproduce`).

---

## 3. Interpreter rules

**Do**
1. Fetch the next node through an activity and return the complete decision (id, type, config, version).
2. Keep a small fixed set of node types in the workflow. Put business kinds inside one activity.
3. Pass node **ids** to activities and let them read the latest config. A config passed as activity input is frozen: retries never see a fix (`q6 config_passed_as_activity_input…`). The fetch decides which node runs; the execute resolves its config and records the version used (`business_safety graph_published_between_fetch_and_execute`).
4. Send a stable idempotency key (`journey:step:node`) with every external call. The ledger alone does not stop a repeat: after a crash between the external call and the ledger write, the retry sent the effect twice without a key and once with it (`business_safety crash_after_side_effect…`).
5. Keep "what ran" in a ledger outside Temporal and make `execute_node` idempotent on `(journey, step)`. This is what prevents re-runs across reset, continue-as-new, goto and terminate + start.
6. Park the journey on a business error and resume it with an Update (`q6 config_error_parks…`).
7. Continue-as-new at a node boundary, after the position is committed, with all handlers finished. Carry ids and unconsumed signals only (`q7`).
8. Put every change to the interpreter's commands behind `workflow.patched()`.
9. Replay captured histories with `Replayer` before every deploy (`test_golden_replay.py`, `q9` R4).

**Don't**
1. Read the graph, a flag or a cache from workflow code.
2. Use the system clock, `random`, env or I/O in workflow code. Use `workflow.now()`, `workflow.uuid4()`.
3. Skip the deprecate step. Removing a `patched()` call while a run recorded with it is still open is an NDE (`q4`, grid below).
4. Start per-participant workflows as children with the default close policy if the parent continues-as-new: they are terminated (`q7 fanout…[TERMINATED]`). Use `ABANDON`, or start them from an activity.
5. Make an NDE fail the workflow. A stuck journey heals; a failed one needs a reset.

**Patch lifecycle, observed** (`q4`; rows = code, columns = history recorded by):

| Code | v1 history | v2-patched history | v3-deprecated history |
|---|---|---|---|
| v1 (old worker during rolling deploy) | OK | NDE | NDE |
| v2 unpatched | NDE | NDE | OK |
| v2 `patched()` | OK | OK | OK |
| v3 `deprecate_patch()` | NDE | OK | OK |
| v4 call removed | NDE | NDE | OK |

Each step is safe only after every run recorded before the previous step has closed or continued-as-new.
Continue-as-new makes that wait one run long instead of one journey long.
To check before deprecating: runs that took the patch carry the search attribute `TemporalChangeVersion`
(observed); any `Running` execution of the workflow type without it still needs the old branch.

**Rolling deploy, observed live** (`q4 rolling_deploy…`): a journey that took the patched branch was picked up
by an old worker, failed its workflow task with an NDE and stayed `Running`; it completed once a new worker
polled. During a rollout this is a temporary stall, not data loss. It lasts until old workers are gone.

**Continue-as-new, observed** (`q7`): a 6-node journey with 2 nodes per run used 3 runs, no node re-ran, and
the last run's history was less than half the size. The new run followed a graph version published during
the previous run. A 3-day wait cut into 1-day chunks rolled over each day and adopted a shortened wait.
A signal received before the rollover was consumed after it, and five signals sent at once while the journey
rolled over five times were all consumed (`q7 signals_arriving_during_continue_as_new…`).

---

## 4. Moving running journeys to a new node

| | A. Update "goto X" | A'. Signal "goto X" | B. Continue-as-new with start X | C. Terminate + start at X |
|---|---|---|---|---|
| Replay-safe | Yes | Yes | Yes | Yes |
| Validation | Rejected if X is missing or already ran | None: a bad target parks the journey | Same as A | None |
| In-flight activity | Finishes; result kept; then moves | Same | Same | Result lost. Side effect still happens outside any workflow. |
| Signals in transit | Kept | Kept | Kept | **Lost** (the send fails) |
| No re-run | Yes | Yes | Yes | Only because of the ledger |
| History | One run | One run | Two linked runs | Terminated run + unrelated new run |
| Other journeys | Unaffected | Unaffected | Unaffected | Unaffected |
| Evidence | `q8 a_update_goto…` (2 tests) | `q8 a_signal_goto…` | `q8 b_continue_as_new…` | `q8 c_terminate_and_start…` |

**Recommendation: A, an Update**, applied at the next node boundary. Add `mode=continue_as_new` (option B)
when the journey should also get a fresh history on current code. Keep C as an operator last resort.

---

## 5. When an NDE happens anyway (`test_q9_nde_response.py`)

Reproduced: a journey parked on v1 code; the worker was replaced with an unpatched change; the next signal
produced `WorkflowTaskFailed: Nondeterminism error … 'fetch_next_node' does not match … 'audit'`; the
journey stayed `Running`; `Replayer` reported the same error offline from the pre-deploy history.

| Remedy | Result | Use when |
|---|---|---|
| 1. Roll back the worker | Journey completed, no re-run | Unintended change. First choice. |
| 2. Redeploy behind `patched()` | Old journey completed on the old branch; a new journey took the new branch; both replay | Intended change |
| 3. Reset to before the divergence | Completed on the new code; the pending signal was re-applied; the ledger stopped the re-run | A few journeys no code can replay |
| 4. Terminate + start | Resumed from the ledger position; a signal sent in between was lost and had to be re-sent | Last resort |

---

## 6. Limits of this proof of concept

- The store and ledger are JSON files with no locking. Production needs transactional storage.
- Replay safety is not business safety. An external system that ignores idempotency keys can still receive a repeat after a crash; that needs an outbox or a check-before-send in the activity.
- An accepted goto lives in workflow history. It survives replay and continue-as-new, but not terminate + start.
- Graphs are acyclic: "already completed" means skip. Plans with intentional loops need a per-pass rule.
- Waiting journeys adopt a changed or removed node only when nudged or at a timer chunk. A publish must nudge them.
- Frequencies in §2 are derived from the measured triggers and SDK defaults, not from production metrics.
- The time-skipping test server does not move tasks off a stopped worker's sticky queue, so those tests run workers without a cache. Child workflows left running also stop its time skipping, so fan-out tests run on the local dev server. The local dev server behaves like production.
