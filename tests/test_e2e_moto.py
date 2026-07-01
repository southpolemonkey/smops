from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from textual.widgets import DataTable
from typer.testing import CliRunner

import sagemaker_ops.cli as cli_module
import sagemaker_ops.tui as tui_module
from sagemaker_ops.aws import (
    build_contexts,
    list_active_pipeline_executions,
    list_pipeline_executions_page,
    list_pipeline_steps,
    list_processing_jobs,
    list_processing_jobs_page,
    tail_step_logs,
)
from sagemaker_ops.cli import app
from sagemaker_ops.tui import PipelineExecutionsApp, ProcessingJobsApp

from .conftest import (
    ACCOUNT_ID,
    REGION,
    context_with_steps,
    create_active_pipeline_execution,
    create_active_processing_job,
    pipeline_steps,
    processing_job_spec,
    seed_failed_step_logs,
)


runner = CliRunner()


def test_processing_submit_and_running_list_cli_with_moto(processing_spec, sagemaker_client):
    result = runner.invoke(
        app,
        [
            "processing",
            "submit",
            "--config",
            str(processing_spec),
            "--region",
            REGION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Processing job 已提交" in result.output
    assert "e2e-processing" in sagemaker_client.describe_processing_job(
        ProcessingJobName="e2e-processing"
    )["ProcessingJobName"]

    create_active_processing_job(sagemaker_client, "running-processing")
    list_result = runner.invoke(app, ["processing", "list", "--region", REGION])

    assert list_result.exit_code == 0, list_result.output
    assert "running-processing" in list_result.output
    assert "InProgress" in list_result.output


def test_processing_list_paginates_running_jobs(sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "running-processing-a")
    create_active_processing_job(sagemaker_client, "running-processing-b")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")

    first_page = list_processing_jobs_page(ctx, page_size=1)

    assert len(first_page.jobs) == 1
    assert first_page.next_token

    second_page = list_processing_jobs_page(ctx, page_size=1, next_token=first_page.next_token)

    assert len(second_page.jobs) == 1
    assert {first_page.jobs[0].name, second_page.jobs[0].name} == {
        "running-processing-a",
        "running-processing-b",
    }

    cli_first_page = runner.invoke(app, ["processing", "list", "--region", REGION, "--max-results", "1"])
    assert cli_first_page.exit_code == 0, cli_first_page.output
    assert "Next token:" in cli_first_page.output


def test_pipeline_start_list_steps_and_failed_logs_with_moto(
    sagemaker_client,
    logs_client,
    monkeypatch,
):
    sagemaker_client.create_pipeline(
        PipelineName="cli-pipeline",
        RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
        PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
    )
    start_result = runner.invoke(
        app,
        [
            "pipeline",
            "start",
            "--name",
            "cli-pipeline",
            "--display-name",
            "cli-run",
            "--parameter",
            "Mode=test",
            "--region",
            REGION,
        ],
    )
    assert start_result.exit_code == 0, start_result.output
    assert "Pipeline execution 已启动" in start_result.output

    execution_arn = sagemaker_client.list_pipeline_executions(PipelineName="cli-pipeline")[
        "PipelineExecutionSummaries"
    ][0]["PipelineExecutionArn"]

    from moto.sagemaker.models import sagemaker_backends

    execution = sagemaker_backends["123456789012"][REGION].pipelines["cli-pipeline"].pipeline_executions[
        execution_arn
    ]
    execution.pipeline_execution_status = "Executing"

    active_result = runner.invoke(app, ["pipeline", "list", "--name", "cli-pipeline", "--region", REGION])
    assert active_result.exit_code == 0, active_result.output
    assert "cli-pipeline" in active_result.output
    assert "Executing" in active_result.output

    ctx = context_with_steps(sagemaker_client, logs_client, execution_arn)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    steps_result = runner.invoke(
        app,
        ["pipeline", "steps", "--execution-arn", execution_arn, "--region", REGION],
    )
    assert steps_result.exit_code == 0, steps_result.output
    assert "PrepareData" in steps_result.output
    assert "ValidateData" in steps_result.output
    assert "Input schema mismatch" in steps_result.output

    seed_failed_step_logs(logs_client)
    failed_step = pipeline_steps(execution_arn)[1]
    assert any("boom: validation failed" in line for line in tail_step_logs(ctx, failed_step))


def test_pipeline_list_includes_recent_finished_execution(sagemaker_client):
    sagemaker_client.create_pipeline(
        PipelineName="idle-pipeline",
        RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
        PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
    )
    sagemaker_client.start_pipeline_execution(PipelineName="idle-pipeline")

    result = runner.invoke(app, ["pipeline", "list", "--name", "idle-pipeline", "--region", REGION])

    assert result.exit_code == 0, result.output
    assert "idle-pipeline" in result.output
    assert "Succeeded" in result.output


def test_pipeline_list_paginates_pipeline_names_without_name(sagemaker_client, logs_client):
    for name in ["paged-pipeline-a", "paged-pipeline-b"]:
        sagemaker_client.create_pipeline(
            PipelineName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
            PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
        )
        sagemaker_client.start_pipeline_execution(PipelineName=name)
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")

    first_page = list_pipeline_executions_page(ctx, pipeline_page_size=1)

    assert len(first_page.executions) == 1
    assert first_page.next_token

    second_page = list_pipeline_executions_page(ctx, pipeline_page_size=1, next_token=first_page.next_token)

    assert len(second_page.executions) == 1
    assert {first_page.executions[0].pipeline_name, second_page.executions[0].pipeline_name} == {
        "paged-pipeline-a",
        "paged-pipeline-b",
    }

    cli_first_page = runner.invoke(app, ["pipeline", "list", "--region", REGION, "--pipeline-page-size", "1"])
    assert cli_first_page.exit_code == 0, cli_first_page.output
    assert "Next token:" in cli_first_page.output


def test_pipeline_list_excludes_finished_execution_outside_recent_window(sagemaker_client):
    sagemaker_client.create_pipeline(
        PipelineName="old-pipeline",
        RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
        PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
    )
    execution_arn = sagemaker_client.start_pipeline_execution(PipelineName="old-pipeline")["PipelineExecutionArn"]

    from moto.sagemaker.models import sagemaker_backends

    execution = sagemaker_backends["123456789012"][REGION].pipelines["old-pipeline"].pipeline_executions[
        execution_arn
    ]
    old_time = datetime.now(timezone.utc) - timedelta(hours=4)
    execution.start_time = old_time
    execution.last_modified_time = old_time
    execution.creation_time = old_time

    result = runner.invoke(app, ["pipeline", "list", "--name", "old-pipeline", "--region", REGION, "--hours", "3"])

    assert result.exit_code == 0, result.output
    assert "没有正在运行或最近 3 小时内结束" in result.output


def test_agent_json_outputs_for_lists_steps_inspect_and_diagnose(
    monkeypatch,
    sagemaker_client,
    logs_client,
):
    create_active_processing_job(sagemaker_client, "json-processing")
    processing_result = runner.invoke(app, ["processing", "list", "--region", REGION, "--json"])

    assert processing_result.exit_code == 0, processing_result.output
    processing_payload = json.loads(processing_result.output)
    assert processing_payload["status"] == "ok"
    assert processing_payload["items"][0]["name"] == "json-processing"
    assert processing_payload["items"][0]["status"] == "InProgress"

    execution_arn = create_active_pipeline_execution(sagemaker_client, name="json-pipeline")
    seed_failed_step_logs(logs_client)
    ctx = context_with_steps(sagemaker_client, logs_client, execution_arn)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    steps_result = runner.invoke(app, ["pipeline", "steps", "--execution-arn", execution_arn, "--json"])
    assert steps_result.exit_code == 0, steps_result.output
    steps_payload = json.loads(steps_result.output)
    assert steps_payload["status"] == "ok"
    assert [step["StepName"] for step in steps_payload["items"]] == ["PrepareData", "ValidateData"]

    inspect_result = runner.invoke(app, ["pipeline", "inspect", "--execution-arn", execution_arn, "--json"])
    assert inspect_result.exit_code == 0, inspect_result.output
    inspect_payload = json.loads(inspect_result.output)
    assert inspect_payload["status"] == "ok"
    assert inspect_payload["execution"]["PipelineName"] == "json-pipeline"
    assert inspect_payload["failed_steps"][0]["StepName"] == "ValidateData"

    diagnose_result = runner.invoke(
        app,
        ["pipeline", "diagnose", "--execution-arn", execution_arn, "--log-limit", "10", "--json"],
    )
    assert diagnose_result.exit_code == 0, diagnose_result.output
    diagnosis = json.loads(diagnose_result.output)
    assert diagnosis["status"] == "ok"
    assert diagnosis["failed_step"]["StepName"] == "ValidateData"
    assert diagnosis["job_type"] == "ProcessingJob"
    assert diagnosis["job_name"] == "failed-processing"
    assert any("boom: validation failed" in line for line in diagnosis["log_tail"])
    assert diagnosis["next_actions"][0]["type"] == "inspect"


def test_wait_commands_emit_json_for_terminal_jobs(sagemaker_client):
    sagemaker_client.create_processing_job(**processing_job_spec("wait-processing"))
    from moto.sagemaker.models import sagemaker_backends

    processing_job = sagemaker_backends[ACCOUNT_ID][REGION].processing_jobs["wait-processing"]
    processing_job.processing_job_status = "Completed"

    processing_result = runner.invoke(
        app,
        ["processing", "wait", "--name", "wait-processing", "--region", REGION, "--timeout", "0", "--json"],
    )
    assert processing_result.exit_code == 0, processing_result.output
    processing_payload = json.loads(processing_result.output)
    assert processing_payload["status"] == "ok"
    assert processing_payload["processing_job"]["status"] == "Completed"

    sagemaker_client.create_pipeline(
        PipelineName="wait-pipeline",
        RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
        PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
    )
    execution_arn = sagemaker_client.start_pipeline_execution(PipelineName="wait-pipeline")["PipelineExecutionArn"]

    pipeline_result = runner.invoke(
        app,
        ["pipeline", "wait", "--execution-arn", execution_arn, "--region", REGION, "--timeout", "0", "--json"],
    )
    assert pipeline_result.exit_code == 0, pipeline_result.output
    pipeline_payload = json.loads(pipeline_result.output)
    assert pipeline_payload["status"] == "ok"
    assert pipeline_payload["execution"]["PipelineExecutionStatus"] == "Succeeded"


def test_build_contexts_supports_multiple_mock_profiles(aws_mock):
    contexts = build_contexts((), None, all_profiles=True)

    assert {ctx.profile for ctx in contexts} == {"mock-dev", "mock-prod"}
    assert {ctx.region for ctx in contexts} == {"us-east-1", "us-west-2"}


def test_aws_helpers_read_active_processing_and_pipeline_state_with_moto(
    sagemaker_client,
    logs_client,
):
    create_active_processing_job(sagemaker_client, "active-processing")
    execution_arn = create_active_pipeline_execution(sagemaker_client)
    ctx = context_with_steps(sagemaker_client, logs_client, execution_arn)

    jobs = list_processing_jobs(ctx)
    executions = list_active_pipeline_executions(ctx, pipeline_name="e2e-pipeline")
    steps = list_pipeline_steps(ctx, execution_arn)

    assert [job.name for job in jobs] == ["active-processing"]
    assert executions[0].pipeline_name == "e2e-pipeline"
    assert executions[0].status == "Executing"
    assert [step["StepName"] for step in steps] == ["PrepareData", "ValidateData"]


def test_processing_tui_shows_running_jobs_and_keyboard_navigation(monkeypatch, sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "processing-a")
    create_active_processing_job(sagemaker_client, "processing-b")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            table = app_under_test.query_one("#jobs", DataTable)
            assert table.row_count == 2
            assert table.cursor_row == 0

            await pilot.press("right")
            assert table.cursor_row == 1
            await pilot.press("left")
            assert table.cursor_row == 0
            await pilot.press("down")
            assert table.cursor_row == 1

            assert app_under_test.jobs[table.cursor_row].name.startswith("processing-")

    asyncio.run(run_app())


def test_pipeline_tui_shows_executions_steps_and_loads_failed_logs(
    monkeypatch,
    sagemaker_client,
    logs_client,
):
    execution_arn = create_active_pipeline_execution(sagemaker_client)
    seed_failed_step_logs(logs_client)
    ctx = context_with_steps(sagemaker_client, logs_client, execution_arn)
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = PipelineExecutionsApp(("mock-dev",), REGION, False, 60, "e2e-pipeline")
        async with app_under_test.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            executions = app_under_test.query_one("#executions", DataTable)
            steps = app_under_test.query_one("#steps", DataTable)
            assert executions.row_count == 1
            assert steps.row_count == 2

            steps.move_cursor(row=1)
            app_under_test.load_selected_step_logs()
            selected = app_under_test.selected_step()
            assert selected is not None
            assert selected["StepName"] == "ValidateData"
            assert app_under_test.loaded_log_step_key == (execution_arn, "ValidateData")
            assert any("boom: validation failed" in line for line in tail_step_logs(ctx, selected))

            app_under_test.action_refresh()
            await pilot.pause()
            assert steps.cursor_row == 1
            assert app_under_test.selected_step()["StepName"] == "ValidateData"
            assert app_under_test.loaded_log_step_key == (execution_arn, "ValidateData")

    asyncio.run(run_app())
