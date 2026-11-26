"""Q6. A node fails on bad configuration, the configuration is fixed (the graph may change too), the journey resumes."""

import uuid

import pytest
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import WorkflowFailureError

from tests.helpers import act, assert_replays, graph, save_history, wait_at, wait_until
from variants.interpreters import ConfigAsActivityInput, FailOnConfigError


def v1_bad():
    return graph(
        "n1",
        n1=act("n2", kind="send_email", template="welcome"),
        n2=act("n3", kind="send_email"),  # bad config: no template
        n3=act(None),
    )


def v2_fixed_and_changed():
    g = v1_bad()
    g["nodes"]["n2"]["config"]["template"] = "reminder"  # the fix
    g["nodes"]["n2"]["next"]["ok"] = "n2b"  # the graph also changed in between
    g["nodes"]["n2b"] = act("n3")
    return g


async def test_config_error_parks_then_resume_continues_on_latest_graph(local):
    local.store.publish(v1_bad())
    async with local.worker(max_cached_workflows=0):  # every task is a replay
        handle = await local.start("park")
        await wait_at(handle, "n2", status="NEEDS_ATTENTION")  # parked, not failed

        local.store.publish(v2_fixed_and_changed())
        assert await handle.execute_update("resume") == "resumed"
        await handle.result()

    assert local.effects("park") == ["n1", "n2", "n2b", "n3"]  # n1 not re-run, new node n2b followed
    assert local.ledger.steps(local.jid("park"))[1]["config"]["template"] == "reminder"
    history = await handle.fetch_history()
    await assert_replays(history)
    save_history("q6_park_and_resume", history)


async def test_config_passed_as_activity_input_retries_never_see_the_fix(local):
    local.store.publish(graph("n1", n1=act(None, kind="send_email")))
    async with local.worker(ConfigAsActivityInput):
        handle = await local.start("input")

        async def attempt():
            pending = (await handle.describe()).raw_description.pending_activities
            return pending[0] if pending else None

        before = await wait_until(lambda: _attempt_at_least(attempt, 2))
        local.store.publish(graph("n1", n1=act(None, kind="send_email", template="welcome")))  # the fix
        after = await wait_until(lambda: _attempt_at_least(attempt, before.attempt + 3))
        await handle.terminate()

    assert "config seen: {}" in after.last_failure.message  # still the original input, fix never seen


async def _attempt_at_least(attempt, n):
    pending = await attempt()
    return pending if pending and pending.attempt >= n else None


@pytest.mark.parametrize("reset_to", ["first_task", "before_failed_node"])
async def test_failed_workflow_reset_resumes_without_rerunning_nodes(local, reset_to):
    local.store.publish(v1_bad())
    name = f"reset-{reset_to}"
    async with local.worker(FailOnConfigError):
        handle = await local.start(name)
        with pytest.raises(WorkflowFailureError):
            await handle.result()  # the journey is closed as FAILED
        failed = await handle.fetch_history()

        local.store.publish(v2_fixed_and_changed())
        event_id = _reset_point(failed, reset_to)
        response = await local.client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=local.client.namespace,
                workflow_execution=WorkflowExecution(workflow_id=handle.id, run_id=failed.run_id),
                reason="config fixed",
                workflow_task_finish_event_id=event_id,
                request_id=str(uuid.uuid4()),
            )
        )
        reset_run = local.client.get_workflow_handle(handle.id, run_id=response.run_id)
        await reset_run.result()

    assert local.effects(name) == ["n1", "n2", "n2b", "n3"]  # n1 ran once even when reset to the start
    await assert_replays(await reset_run.fetch_history(), FailOnConfigError)


def _reset_point(history, reset_to):
    completed = [e.event_id for e in history.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED]
    if reset_to == "first_task":
        return completed[0]
    # the workflow task that scheduled the failing execute_node(n2)
    scheduled = [
        e for e in history.events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        and e.activity_task_scheduled_event_attributes.activity_type.name == "execute_node"
    ]
    return scheduled[-1].activity_task_scheduled_event_attributes.workflow_task_completed_event_id
