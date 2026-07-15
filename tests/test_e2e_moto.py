from __future__ import annotations

import asyncio
import boto3
import json
import time
from datetime import datetime, timedelta, timezone
from threading import Lock

from textual.widgets import DataTable, RichLog, Static
from botocore.stub import Stubber
from typer.testing import CliRunner

import sagemaker_ops.cli as cli_module
import sagemaker_ops.tui as tui_module
from sagemaker_ops.aws import (
    AirflowDagView,
    AirflowDagRunView,
    AirflowPoolView,
    AwsCliError,
    AwsContext,
    EcsClusterView,
    EcsServiceView,
    EcsTaskView,
    build_contexts,
    list_active_pipeline_executions,
    list_airflow_dag_runs,
    list_airflow_dags,
    list_airflow_pools,
    list_mwaa_environments,
    list_ecs_clusters,
    list_ecs_services,
    list_ecs_tasks,
    list_pipeline_executions_page,
    list_pipeline_steps,
    list_processing_jobs,
    list_processing_jobs_page,
    resolve_ecs_task_log_source,
    tail_ecs_task_logs,
    tail_processing_job_logs,
    tail_step_logs,
)
import sagemaker_ops.aws as aws_module
from sagemaker_ops.cli import app
from sagemaker_ops.tui import AirflowApp, EcsTasksApp, PipelineExecutionsApp, ProcessingJobsApp, SmopsTuiApp

from .conftest import (
    ACCOUNT_ID,
    REGION,
    context_with_steps,
    create_active_pipeline_execution,
    create_active_processing_job,
    pipeline_steps,
    processing_job_spec,
    seed_failed_step_logs,
    stubbed_mwaa_context,
)


runner = CliRunner()


class NoDescribePipelineClient:
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        return getattr(self.wrapped, name)

    def describe_pipeline_execution(self, **_kwargs):
        raise AssertionError("pipeline listing should not describe each execution")


class SlowPipelineListClient:
    def __init__(self, wrapped, delay_seconds: float = 0.05):
        self.wrapped = wrapped
        self.delay_seconds = delay_seconds
        self.active_calls = 0
        self.max_active_calls = 0
        self.lock = Lock()

    def __getattr__(self, name):
        return getattr(self.wrapped, name)

    def list_pipeline_executions(self, **kwargs):
        with self.lock:
            self.active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self.active_calls)
        try:
            time.sleep(self.delay_seconds)
            return self.wrapped.list_pipeline_executions(**kwargs)
        finally:
            with self.lock:
                self.active_calls -= 1


def stubbed_ecs_context():
    ecs = boto3.client("ecs", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    return AwsContext("mock-dev", REGION, None, logs, ecs), Stubber(ecs), Stubber(logs)


def test_ecs_helpers_list_clusters_services_tasks_and_logs():
    ctx, ecs_stubber, logs_stubber = stubbed_ecs_context()
    cluster_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:cluster/demo"
    service_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:service/demo/api"
    task_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task/demo/abc123"
    task_definition_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task-definition/demo:1"

    ecs_stubber.add_response("list_clusters", {"clusterArns": [cluster_arn]}, {})
    ecs_stubber.add_response(
        "describe_clusters",
        {
            "clusters": [
                {
                    "clusterArn": cluster_arn,
                    "clusterName": "demo",
                    "status": "ACTIVE",
                    "runningTasksCount": 1,
                    "pendingTasksCount": 0,
                    "activeServicesCount": 1,
                }
            ]
        },
        {"clusters": [cluster_arn]},
    )
    ecs_stubber.add_response("list_services", {"serviceArns": [service_arn]}, {"cluster": "demo"})
    ecs_stubber.add_response(
        "describe_services",
        {
            "services": [
                {
                    "serviceArn": service_arn,
                    "serviceName": "api",
                    "status": "ACTIVE",
                    "desiredCount": 1,
                    "runningCount": 1,
                    "pendingCount": 0,
                    "taskDefinition": task_definition_arn,
                }
            ]
        },
        {"cluster": "demo", "services": [service_arn]},
    )
    ecs_stubber.add_response(
        "list_tasks",
        {"taskArns": [task_arn]},
        {"cluster": "demo", "desiredStatus": "RUNNING", "serviceName": "api"},
    )
    ecs_stubber.add_response(
        "describe_tasks",
        {
            "tasks": [
                {
                    "taskArn": task_arn,
                    "clusterArn": cluster_arn,
                    "taskDefinitionArn": task_definition_arn,
                    "lastStatus": "RUNNING",
                    "desiredStatus": "RUNNING",
                    "launchType": "FARGATE",
                    "startedAt": datetime(2026, 7, 2, 1, 2, tzinfo=timezone.utc),
                    "containers": [{"name": "api", "lastStatus": "RUNNING", "runtimeId": "runtime-1"}],
                }
            ]
        },
        {"cluster": "demo", "tasks": [task_arn]},
    )
    ecs_stubber.add_response(
        "describe_tasks",
        {
            "tasks": [
                {
                    "taskArn": task_arn,
                    "clusterArn": cluster_arn,
                    "taskDefinitionArn": task_definition_arn,
                    "lastStatus": "RUNNING",
                    "desiredStatus": "RUNNING",
                    "launchType": "FARGATE",
                    "startedAt": datetime(2026, 7, 2, 1, 2, tzinfo=timezone.utc),
                    "containers": [{"name": "api", "lastStatus": "RUNNING", "runtimeId": "runtime-1"}],
                }
            ]
        },
        {"cluster": "demo", "tasks": [task_arn]},
    )
    ecs_stubber.add_response(
        "describe_task_definition",
        {
            "taskDefinition": {
                "taskDefinitionArn": task_definition_arn,
                "family": "demo",
                "containerDefinitions": [
                    {
                        "name": "api",
                        "image": "example.com/api:latest",
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": "/ecs/demo",
                                "awslogs-region": REGION,
                                "awslogs-stream-prefix": "ecs",
                            },
                        },
                    }
                ],
            }
        },
        {"taskDefinition": task_definition_arn},
    )
    logs_stubber.add_response(
        "describe_log_streams",
        {"logStreams": [{"logStreamName": "ecs/api/abc123", "lastEventTimestamp": 2000}]},
        {"logGroupName": "/ecs/demo", "logStreamNamePrefix": "ecs/api/abc123", "limit": 5},
    )
    logs_stubber.add_response(
        "get_log_events",
        {"events": [{"timestamp": 2000, "message": "api ready"}]},
        {"logGroupName": "/ecs/demo", "logStreamName": "ecs/api/abc123", "limit": 80, "startFromHead": False},
    )

    with ecs_stubber, logs_stubber:
        clusters = list_ecs_clusters(ctx)
        services = list_ecs_services(ctx, "demo")
        tasks = list_ecs_tasks(ctx, "demo", service="api")
        lines = tail_ecs_task_logs(ctx, "demo", task_arn)

    assert clusters[0].cluster_name == "demo"
    assert services[0].service_name == "api"
    assert tasks[0].task_id == "abc123"
    assert any("api ready" in line for line in lines)


def test_ecs_log_source_requires_container_when_multiple_awslogs_containers():
    ctx = AwsContext("mock-dev", REGION, None, None, None)
    task = EcsTaskView(
        profile="mock-dev",
        region=REGION,
        cluster="demo",
        task_arn=f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task/demo/abc123",
        task_id="abc123",
        task_definition_arn="demo:1",
        last_status="RUNNING",
        desired_status="RUNNING",
        launch_type="FARGATE",
        started_at=None,
        stopped_at=None,
        stopped_reason="",
        containers=[],
    )

    class FakeEcs:
        def describe_task_definition(self, taskDefinition):
            return {
                "taskDefinition": {
                    "containerDefinitions": [
                        {
                            "name": "api",
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {"awslogs-group": "/ecs/demo", "awslogs-stream-prefix": "ecs"},
                            },
                        },
                        {
                            "name": "worker",
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {"awslogs-group": "/ecs/demo", "awslogs-stream-prefix": "ecs"},
                            },
                        },
                    ]
                }
            }

    ctx = AwsContext(ctx.profile, ctx.region, ctx.sagemaker, ctx.logs, FakeEcs())

    try:
        resolve_ecs_task_log_source(ctx, task)
    except AwsCliError as exc:
        assert "--container" in str(exc)
    else:
        raise AssertionError("expected AwsCliError")


def test_ecs_cli_clusters_json(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_args, **_kwargs: [ctx])
    monkeypatch.setattr(
        cli_module,
        "list_ecs_clusters",
        lambda _ctx: [
            EcsClusterView("mock-dev", REGION, "arn:cluster/demo", "demo", "ACTIVE", 2, 0, 1)
        ],
    )

    result = runner.invoke(app, ["ecs", "clusters", "--region", REGION, "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["count"] == 1
    assert payload["items"][0]["cluster_name"] == "demo"


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


def test_processing_job_logs_tail_from_cloudwatch(sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "processing-with-logs")
    seed_failed_step_logs(logs_client, "processing-with-logs")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")

    lines = tail_processing_job_logs(ctx, "processing-with-logs")

    assert any("boom: validation failed" in line for line in lines)


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


def test_pipeline_list_uses_summaries_without_describing_each_execution(sagemaker_client, logs_client):
    for name in ["summary-pipeline-a", "summary-pipeline-b"]:
        sagemaker_client.create_pipeline(
            PipelineName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
            PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
        )
        sagemaker_client.start_pipeline_execution(PipelineName=name)
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    ctx = ctx.__class__(ctx.profile, ctx.region, NoDescribePipelineClient(ctx.sagemaker), ctx.logs)

    page = list_pipeline_executions_page(ctx, pipeline_page_size=2)

    assert len(page.executions) == 2


def test_pipeline_list_scans_pipeline_executions_concurrently(sagemaker_client, logs_client):
    for index in range(4):
        name = f"concurrent-pipeline-{index}"
        sagemaker_client.create_pipeline(
            PipelineName=name,
            RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
            PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
        )
        sagemaker_client.start_pipeline_execution(PipelineName=name)
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    slow_client = SlowPipelineListClient(ctx.sagemaker)
    ctx = ctx.__class__(ctx.profile, ctx.region, slow_client, ctx.logs)

    page = list_pipeline_executions_page(ctx, pipeline_page_size=4)

    assert len(page.executions) == 4
    assert slow_client.max_active_calls > 1


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


def test_smops_default_region_config_is_used_by_cli(monkeypatch, tmp_path):
    config_file = tmp_path / "smops-config.json"
    monkeypatch.setenv("SMOPS_CONFIG_FILE", str(config_file))

    set_result = runner.invoke(app, ["config", "set-region", "ap-southeast-2", "--json"])
    assert set_result.exit_code == 0, set_result.output
    set_payload = json.loads(set_result.output)
    assert set_payload["default_region"] == "ap-southeast-2"
    assert config_file.exists()

    get_result = runner.invoke(app, ["config", "get-region", "--json"])
    assert get_result.exit_code == 0, get_result.output
    assert json.loads(get_result.output)["default_region"] == "ap-southeast-2"

    seen = {}

    def fake_build_contexts(profiles, region, all_profiles=False):
        seen["profiles"] = profiles
        seen["region"] = region
        seen["all_profiles"] = all_profiles
        return []

    monkeypatch.setattr(cli_module, "build_contexts", fake_build_contexts)
    list_result = runner.invoke(app, ["processing", "list", "--json"])

    assert list_result.exit_code == 0, list_result.output
    assert seen["region"] == "ap-southeast-2"


def test_smops_default_region_env_overrides_config(monkeypatch, tmp_path):
    config_file = tmp_path / "smops-config.json"
    monkeypatch.setenv("SMOPS_CONFIG_FILE", str(config_file))
    assert runner.invoke(app, ["config", "set-region", "us-east-1"]).exit_code == 0
    monkeypatch.setenv("SMOPS_DEFAULT_REGION", "ap-southeast-2")

    result = runner.invoke(app, ["config", "get-region", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["default_region"] == "ap-southeast-2"


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


def test_smops_tui_home_selects_processing_view():
    async def run_app() -> None:
        app_under_test = SmopsTuiApp()
        async with app_under_test.run_test(size=(100, 30)) as pilot:
            await pilot.pause()
            table = app_under_test.query_one("#home", DataTable)
            assert table.row_count == 4
            await pilot.press("down")
            await pilot.press("enter")
            assert app_under_test.return_value == "processing"

    asyncio.run(run_app())


def test_processing_tui_switches_profile_with_picker(monkeypatch, sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "profile-processing")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "available_profiles", lambda: ["mock-dev", "mock-prod"])
    monkeypatch.setattr(tui_module, "build_contexts", lambda profiles, *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            await pilot.press("p")
            await pilot.pause()
            assert app_under_test.screen.__class__.__name__ == "ProfileSelectScreen"
            profiles_view = app_under_test.screen.query_one("#profiles", Static)
            assert "mock-dev (current)" in str(profiles_view.content)
            assert "mock-prod" in str(profiles_view.content)
            assert app_under_test.profiles == ("mock-dev",)
            await pilot.press("down")
            await pilot.press("enter")
            await pilot.pause()
            assert app_under_test.profiles == ("mock-prod",)
            assert app_under_test.all_profiles is False

    asyncio.run(run_app())


def test_processing_tui_profile_picker_keeps_cursor_visible_at_end(monkeypatch, sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "profile-processing")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    profiles = [f"mock-{index:02d}" for index in range(25)]
    monkeypatch.setattr(tui_module, "available_profiles", lambda: profiles)
    monkeypatch.setattr(tui_module, "build_contexts", lambda profile_names, *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-00",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            await pilot.press("p")
            await pilot.pause()
            for _ in range(30):
                await pilot.press("down")
            await pilot.pause()
            profiles_view = app_under_test.screen.query_one("#profiles", Static)
            rendered = str(profiles_view.content)
            assert "> mock-24" in rendered
            assert "mock-00 (current)" not in rendered
            await pilot.press("enter")
            await pilot.pause()
            assert app_under_test.profiles == ("mock-24",)

    asyncio.run(run_app())


def test_processing_tui_keeps_current_profile_when_selected_profile_fails(monkeypatch, sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "profile-processing")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "available_profiles", lambda: ["mock-dev", "bad-sso", "mock-prod"])

    def fake_build_contexts(profiles, *_args, **_kwargs):
        if profiles == ("bad-sso",):
            raise AwsCliError("custom-process: SSO ForbiddenException: No access")
        return [ctx]

    monkeypatch.setattr(tui_module, "build_contexts", fake_build_contexts)

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            await pilot.press("p")
            await pilot.pause()
            await pilot.press("down")
            await pilot.press("enter")
            await pilot.pause()
            assert app_under_test.profiles == ("mock-dev",)
            status = app_under_test.query_one("#status", Static).content
            assert "ForbiddenException" in str(status)

    asyncio.run(run_app())


def test_smops_tui_home_selects_ecs_view():
    async def run_app() -> None:
        app_under_test = SmopsTuiApp()
        async with app_under_test.run_test(size=(100, 30)) as pilot:
            await pilot.pause()
            table = app_under_test.query_one("#home", DataTable)
            assert table.row_count == 4
            await pilot.press("down")
            await pilot.press("down")
            await pilot.press("enter")
            assert app_under_test.return_value == "ecs"

    asyncio.run(run_app())


def test_ecs_tui_shows_clusters_services_tasks_and_loads_logs(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None)
    cluster = EcsClusterView("mock-dev", REGION, "arn:cluster/demo", "demo", "ACTIVE", 1, 0, 1)
    service = EcsServiceView("mock-dev", REGION, "demo", "arn:service/api", "api", "ACTIVE", 1, 1, 0, "td:1")
    task = EcsTaskView(
        profile="mock-dev",
        region=REGION,
        cluster="demo",
        task_arn="arn:task/demo/abc123",
        task_id="abc123",
        task_definition_arn="td:1",
        last_status="RUNNING",
        desired_status="RUNNING",
        launch_type="FARGATE",
        started_at=datetime(2026, 7, 2, 1, 2, tzinfo=timezone.utc),
        stopped_at=None,
        stopped_reason="",
        containers=[{"name": "api", "last_status": "RUNNING"}],
    )
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])
    monkeypatch.setattr(tui_module, "list_ecs_clusters", lambda _ctx: [cluster])
    monkeypatch.setattr(tui_module, "list_ecs_services", lambda _ctx, _cluster: [service])
    monkeypatch.setattr(tui_module, "list_ecs_tasks", lambda _ctx, _cluster, service=None, desired_status="RUNNING": [task])
    monkeypatch.setattr(tui_module, "tail_ecs_task_logs", lambda *_args, **_kwargs: ["api ready"])

    async def run_app() -> None:
        app_under_test = EcsTasksApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            clusters = app_under_test.query_one("#ecs-clusters", DataTable)
            services = app_under_test.query_one("#ecs-services", DataTable)
            tasks = app_under_test.query_one("#ecs-tasks", DataTable)
            assert clusters.row_count == 1
            assert services.row_count == 1
            assert tasks.row_count == 1

            app_under_test.load_selected_ecs_logs()
            await pilot.pause()

            assert app_under_test.loaded_ecs_log_key == ("mock-dev", REGION, "demo", "arn:task/demo/abc123")
            assert app_under_test.selected_ecs_task().task_id == "abc123"

    asyncio.run(run_app())


def test_tui_start_shortcuts_open_forms(monkeypatch, sagemaker_client, logs_client):
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_processing() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            await pilot.press("s")
            await pilot.pause()
            assert app_under_test.screen.__class__.__name__ == "ProcessingSubmitScreen"

    async def run_pipeline() -> None:
        app_under_test = PipelineExecutionsApp(("mock-dev",), REGION, False, 60, "startable-pipeline")
        async with app_under_test.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            await pilot.press("s")
            await pilot.pause()
            assert app_under_test.screen.__class__.__name__ == "PipelineStartScreen"

    asyncio.run(run_processing())
    asyncio.run(run_pipeline())


def test_pipeline_tui_can_start_pipeline_from_form_callback(monkeypatch, sagemaker_client, logs_client):
    sagemaker_client.create_pipeline(
        PipelineName="startable-pipeline",
        RoleArn="arn:aws:iam::123456789012:role/SageMakerExecutionRole",
        PipelineDefinition='{"Version": "2020-12-01", "Steps": []}',
    )
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = PipelineExecutionsApp(("mock-dev",), REGION, False, 60, "startable-pipeline")
        async with app_under_test.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            app_under_test.start_pipeline_from_form(
                {
                    "pipeline_name": "startable-pipeline",
                    "display_name": "from-tui",
                    "parameters": "Mode=test",
                }
            )
            await pilot.pause()
            executions = sagemaker_client.list_pipeline_executions(PipelineName="startable-pipeline")[
                "PipelineExecutionSummaries"
            ]
            assert executions

    asyncio.run(run_app())


def test_processing_tui_can_submit_processing_from_form_callback(monkeypatch, tmp_path, sagemaker_client, logs_client):
    spec_path = tmp_path / "tui-processing.json"
    spec_path.write_text(json.dumps(processing_job_spec("tui-processing")), encoding="utf-8")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            app_under_test.submit_processing_from_form(str(spec_path))
            await pilot.pause()
            detail = sagemaker_client.describe_processing_job(ProcessingJobName="tui-processing")
            assert detail["ProcessingJobName"] == "tui-processing"

    asyncio.run(run_app())


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


def test_processing_tui_loads_selected_job_logs(monkeypatch, sagemaker_client, logs_client):
    create_active_processing_job(sagemaker_client, "processing-with-logs")
    seed_failed_step_logs(logs_client, "processing-with-logs")
    ctx = context_with_steps(sagemaker_client, logs_client, "unused")
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_args, **_kwargs: [ctx])

    async def run_app() -> None:
        app_under_test = ProcessingJobsApp(("mock-dev",), REGION, False, 60)
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause()
            assert app_under_test.query_one("#detail", Static)
            assert app_under_test.query_one("#processing-logs", RichLog)
            table = app_under_test.query_one("#jobs", DataTable)
            assert table.row_count == 1

            app_under_test.load_selected_processing_logs()
            await pilot.pause()

            assert app_under_test.loaded_processing_log_key == ("mock-dev", REGION, "processing-with-logs")
            assert any("boom: validation failed" in line for line in tail_processing_job_logs(ctx, "processing-with-logs"))

    asyncio.run(run_app())


def test_pipeline_tui_shows_loading_state_before_refresh_finishes(monkeypatch):
    def slow_build_contexts(*_args, **_kwargs):
        time.sleep(0.2)
        return []

    monkeypatch.setattr(tui_module, "build_contexts", slow_build_contexts)

    async def run_app() -> None:
        app_under_test = PipelineExecutionsApp(("mock-dev",), REGION, False, 60, "slow-pipeline")
        async with app_under_test.run_test(size=(140, 40)) as pilot:
            await pilot.pause(0.01)
            status = app_under_test.query_one("#status", Static).content
            assert "Loading pipeline executions" in str(status)

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


# --- Airflow / MWAA ---------------------------------------------------------


def _airflow_dag_view(dag_id: str = "avm_end_to_end", paused: bool = False) -> AirflowDagView:
    return AirflowDagView(
        profile="mock-dev",
        region=REGION,
        environment="u-airflow2",
        dag_id=dag_id,
        is_paused=paused,
        is_active=True,
        schedule_interval="0 * * * *",
        owners=["airflow"],
        tags=["avm"],
        description="AVM end to end",
    )


def _airflow_run_view(dag_id: str = "avm_end_to_end", run_id: str = "manual__2026-01-01") -> AirflowDagRunView:
    return AirflowDagRunView(
        profile="mock-dev",
        region=REGION,
        environment="u-airflow2",
        dag_id=dag_id,
        dag_run_id=run_id,
        state="success",
        run_type="manual",
        logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        start_date=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_date=datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
        external_trigger=True,
    )


def test_list_mwaa_environments_via_stubber():
    ctx, stubber = stubbed_mwaa_context()
    stubber.add_response("list_environments", {"Environments": ["u-airflow2"]}, {})
    stubber.add_response(
        "get_environment",
        {
            "Environment": {
                "Name": "u-airflow2",
                "Status": "AVAILABLE",
                "AirflowVersion": "2.7.2",
                "WebserverUrl": "https://example-vpce.airflow.amazonaws.com",
                "Schedulers": 2,
            }
        },
        {"Name": "u-airflow2"},
    )

    with stubber:
        environments = list_mwaa_environments(ctx)

    assert len(environments) == 1
    assert environments[0].name == "u-airflow2"
    assert environments[0].status == "AVAILABLE"
    assert environments[0].airflow_version == "2.7.2"
    assert environments[0].schedulers == 2


def test_list_airflow_dags_parses_rest_payload(monkeypatch):
    ctx, _ = stubbed_mwaa_context()
    monkeypatch.setattr(aws_module, "_airflow_session", lambda _ctx, _env: (None, "host"))
    payload = {
        "dags": [
            {
                "dag_id": "avm_end_to_end",
                "is_paused": False,
                "is_active": True,
                "schedule_interval": {"__type": "CronExpression", "value": "0 * * * *"},
                "owners": ["airflow"],
                "tags": [{"name": "avm"}],
                "description": "AVM end to end",
            }
        ]
    }
    monkeypatch.setattr(aws_module, "_airflow_get", lambda _s, _h, _path, params=None: payload)

    dags = list_airflow_dags(ctx, "u-airflow2", name_pattern="avm")

    assert len(dags) == 1
    assert dags[0].dag_id == "avm_end_to_end"
    assert dags[0].schedule_interval == "0 * * * *"
    assert dags[0].tags == ["avm"]
    assert dags[0].owners == ["airflow"]


def test_list_airflow_dag_runs_parses_states(monkeypatch):
    ctx, _ = stubbed_mwaa_context()
    monkeypatch.setattr(aws_module, "_airflow_session", lambda _ctx, _env: (None, "host"))
    payload = {
        "dag_runs": [
            {
                "dag_id": "avm_end_to_end",
                "dag_run_id": "manual__2026-01-01",
                "state": "failed",
                "run_type": "manual",
                "logical_date": "2026-01-01T00:00:00+00:00",
                "start_date": "2026-01-01T00:00:00+00:00",
                "end_date": "2026-01-01T00:10:00+00:00",
                "external_trigger": True,
            }
        ]
    }
    monkeypatch.setattr(aws_module, "_airflow_get", lambda _s, _h, _path, params=None: payload)

    runs = list_airflow_dag_runs(ctx, "u-airflow2", "avm_end_to_end")

    assert len(runs) == 1
    assert runs[0].state == "failed"
    assert runs[0].external_trigger is True
    assert runs[0].start_date == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_list_airflow_pools_parses_slots(monkeypatch):
    ctx, _ = stubbed_mwaa_context()
    monkeypatch.setattr(aws_module, "_airflow_session", lambda _ctx, _env: (None, "host"))
    payload = {
        "pools": [
            {
                "name": "default_pool",
                "slots": 10000,
                "occupied_slots": 20,
                "running_slots": 0,
                "queued_slots": 20,
                "open_slots": 9980,
            }
        ]
    }
    monkeypatch.setattr(aws_module, "_airflow_get", lambda _s, _h, _path, params=None: payload)

    pools = list_airflow_pools(ctx, "u-airflow2")

    assert len(pools) == 1
    assert pools[0].name == "default_pool"
    assert pools[0].open_slots == 9980


def test_airflow_cli_runs_json(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.setattr(cli_module, "list_airflow_dag_runs", lambda _ctx, _env, _dag, limit=10: [_airflow_run_view()])

    result = runner.invoke(
        app,
        ["airflow", "runs", "--dag", "avm_end_to_end", "--env", "u-airflow2", "--region", REGION, "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["environment"] == "u-airflow2"
    assert payload["count"] == 1
    assert payload["items"][0]["dag_run_id"] == "manual__2026-01-01"


def test_airflow_cli_dags_table(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.setattr(cli_module, "list_airflow_dags", lambda _ctx, _env, name_pattern=None, limit=100: [_airflow_dag_view()])

    result = runner.invoke(app, ["airflow", "dags", "--env", "u-airflow2", "--region", REGION])

    assert result.exit_code == 0, result.output
    assert "avm_end_to_end" in result.output


def test_airflow_cli_requires_environment(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.delenv("SMOPS_MWAA_ENVIRONMENT", raising=False)

    result = runner.invoke(app, ["airflow", "pools", "--region", REGION, "--json"])

    assert result.exit_code == 1, result.output
    assert "No MWAA environment" in result.output


def test_airflow_trigger_confirms_and_triggers_with_yes(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    seen = {}

    def fake_trigger(_ctx, env, dag, conf=None, logical_date=None):
        seen["env"] = env
        seen["dag"] = dag
        seen["conf"] = conf
        return _airflow_run_view(dag_id=dag, run_id="manual__triggered")

    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.setattr(cli_module, "trigger_airflow_dag", fake_trigger)

    result = runner.invoke(
        app,
        [
            "airflow",
            "trigger",
            "--dag",
            "avm_end_to_end",
            "--env",
            "u-airflow2",
            "--region",
            REGION,
            "--conf",
            '{"run_date": "2026-01-01"}',
            "--yes",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dag_run"]["dag_run_id"] == "manual__triggered"
    assert seen["conf"] == {"run_date": "2026-01-01"}


def test_airflow_trigger_aborts_when_not_confirmed(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    called = {"triggered": False}

    def fake_trigger(*_a, **_k):
        called["triggered"] = True
        return _airflow_run_view()

    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.setattr(cli_module, "trigger_airflow_dag", fake_trigger)

    result = runner.invoke(
        app,
        ["airflow", "trigger", "--dag", "avm_end_to_end", "--env", "u-airflow2", "--region", REGION],
        input="n\n",
    )

    assert result.exit_code == 0, result.output
    assert "Aborted" in result.output
    assert called["triggered"] is False


def test_airflow_trigger_json_requires_yes(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    monkeypatch.setattr(cli_module, "build_contexts", lambda *_a, **_k: [ctx])

    result = runner.invoke(
        app,
        ["airflow", "trigger", "--dag", "avm_end_to_end", "--env", "u-airflow2", "--region", REGION, "--json"],
    )

    assert result.exit_code == 1, result.output
    assert "--yes" in result.output


def test_smops_default_mwaa_env_config_round_trip(monkeypatch, tmp_path):
    config_file = tmp_path / "smops-config.json"
    monkeypatch.setenv("SMOPS_CONFIG_FILE", str(config_file))
    monkeypatch.delenv("SMOPS_MWAA_ENVIRONMENT", raising=False)

    set_result = runner.invoke(app, ["config", "set-mwaa-env", "u-airflow2", "--json"])
    assert set_result.exit_code == 0, set_result.output
    assert json.loads(set_result.output)["mwaa_environment"] == "u-airflow2"

    get_result = runner.invoke(app, ["config", "get-mwaa-env", "--json"])
    assert get_result.exit_code == 0, get_result.output
    assert json.loads(get_result.output)["mwaa_environment"] == "u-airflow2"


def test_smops_tui_home_selects_airflow_view():
    async def run_app() -> None:
        app_under_test = SmopsTuiApp()
        async with app_under_test.run_test(size=(100, 30)) as pilot:
            await pilot.pause()
            table = app_under_test.query_one("#home", DataTable)
            assert table.row_count == 4
            await pilot.press("down")
            await pilot.press("down")
            await pilot.press("down")
            await pilot.press("enter")
            assert app_under_test.return_value == "airflow"

    asyncio.run(run_app())


def test_airflow_tui_shows_dags(monkeypatch):
    ctx = AwsContext("mock-dev", REGION, None, None, None, None)
    monkeypatch.setattr(tui_module, "build_contexts", lambda *_a, **_k: [ctx])
    monkeypatch.setattr(tui_module, "list_airflow_dags", lambda _ctx, _env: [_airflow_dag_view()])
    monkeypatch.setattr(tui_module, "list_airflow_pools", lambda _ctx, _env: [
        AirflowPoolView("mock-dev", REGION, "u-airflow2", "default_pool", 10000, 20, 0, 20, 9980)
    ])
    monkeypatch.setattr(tui_module, "list_airflow_dag_runs", lambda _ctx, _env, _dag: [_airflow_run_view()])

    async def run_app() -> None:
        app_under_test = AirflowApp(("mock-dev",), REGION, False, 60, "u-airflow2")
        async with app_under_test.run_test(size=(160, 50)) as pilot:
            await pilot.pause()
            dags = app_under_test.query_one("#airflow-dags", DataTable)
            assert dags.row_count == 1
            assert app_under_test.selected_dag().dag_id == "avm_end_to_end"

    asyncio.run(run_app())
