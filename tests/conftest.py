from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pytest
from rich.console import Console
from moto import mock_aws
from moto.sagemaker.models import sagemaker_backends

from sagemaker_ops.aws import AwsContext


ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/SageMakerExecutionRole"


@pytest.fixture(autouse=True)
def fake_aws_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = tmp_path / "config"
    credentials = tmp_path / "credentials"
    config.write_text(
        "\n".join(
            [
                "[profile mock-dev]",
                f"region = {REGION}",
                "output = json",
                "",
                "[profile mock-prod]",
                "region = us-west-2",
                "output = json",
                "",
            ]
        ),
        encoding="utf-8",
    )
    credentials.write_text(
        "\n".join(
            [
                "[mock-dev]",
                "aws_access_key_id = testing",
                "aws_secret_access_key = testing",
                "",
                "[mock-prod]",
                "aws_access_key_id = testing",
                "aws_secret_access_key = testing",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture(autouse=True)
def wide_cli_console(monkeypatch: pytest.MonkeyPatch) -> None:
    import sagemaker_ops.cli as cli_module

    monkeypatch.setattr(cli_module, "console", Console(width=220, force_terminal=False))


@pytest.fixture
def aws_mock() -> Iterator[None]:
    with mock_aws():
        yield


@pytest.fixture
def sagemaker_client(aws_mock: None) -> Any:
    return boto3.client("sagemaker", region_name=REGION)


@pytest.fixture
def logs_client(aws_mock: None) -> Any:
    return boto3.client("logs", region_name=REGION)


@pytest.fixture
def processing_spec(tmp_path: Path) -> Path:
    path = tmp_path / "processing-job.json"
    path.write_text(json.dumps(processing_job_spec("e2e-processing")), encoding="utf-8")
    return path


def processing_job_spec(name: str) -> dict[str, Any]:
    return {
        "ProcessingJobName": name,
        "RoleArn": ROLE_ARN,
        "AppSpecification": {
            "ImageUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/processing:latest",
        },
        "ProcessingResources": {
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.xlarge",
                "VolumeSizeInGB": 30,
            },
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
    }


def create_active_processing_job(client: Any, name: str) -> None:
    client.create_processing_job(**processing_job_spec(name))
    job = sagemaker_backends[ACCOUNT_ID][REGION].processing_jobs[name]
    job.processing_job_status = "InProgress"
    job.processing_end_time = None


def create_active_pipeline_execution(client: Any, name: str = "e2e-pipeline") -> str:
    client.create_pipeline(
        PipelineName=name,
        RoleArn=ROLE_ARN,
        PipelineDefinition=json.dumps({"Version": "2020-12-01", "Steps": []}),
    )
    response = client.start_pipeline_execution(
        PipelineName=name,
        PipelineExecutionDisplayName="manual-e2e",
        PipelineParameters=[{"Name": "Mode", "Value": "test"}],
    )
    execution_arn = response["PipelineExecutionArn"]
    execution = sagemaker_backends[ACCOUNT_ID][REGION].pipelines[name].pipeline_executions[execution_arn]
    execution.pipeline_execution_status = "Executing"
    execution.pipeline_execution_failure_reason = None
    return execution_arn


def seed_failed_step_logs(client: Any, job_name: str = "failed-processing") -> None:
    log_group = "/aws/sagemaker/ProcessingJobs"
    log_stream = f"{job_name}/algo-1-1234567890"
    client.create_log_group(logGroupName=log_group)
    client.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    client.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=[
            {"timestamp": now_ms, "message": "loading input"},
            {"timestamp": now_ms + 1000, "message": "boom: validation failed"},
        ],
    )


def pipeline_steps(execution_arn: str) -> list[dict[str, Any]]:
    _ = execution_arn
    started = datetime(2026, 6, 30, 13, 0, tzinfo=timezone.utc)
    return [
        {
            "StepName": "PrepareData",
            "StepType": "Processing",
            "StepStatus": "Executing",
            "StartTime": started,
            "Metadata": {
                "ProcessingJob": {
                    "Arn": f"arn:aws:sagemaker:{REGION}:{ACCOUNT_ID}:processing-job/prepare-data",
                }
            },
        },
        {
            "StepName": "ValidateData",
            "StepType": "Processing",
            "StepStatus": "Failed",
            "StartTime": started.replace(minute=5),
            "EndTime": started.replace(minute=7),
            "FailureReason": "Input schema mismatch",
            "Metadata": {
                "ProcessingJob": {
                    "Arn": f"arn:aws:sagemaker:{REGION}:{ACCOUNT_ID}:processing-job/failed-processing",
                }
            },
        },
    ]


class FakePipelineStepsPaginator:
    def __init__(self, steps_by_execution: dict[str, list[dict[str, Any]]]) -> None:
        self.steps_by_execution = steps_by_execution

    def paginate(self, PipelineExecutionArn: str, **_: Any) -> list[dict[str, Any]]:
        return [{"PipelineExecutionSteps": self.steps_by_execution.get(PipelineExecutionArn, [])}]


class SageMakerWithPipelineSteps:
    def __init__(self, wrapped: Any, steps_by_execution: dict[str, list[dict[str, Any]]]) -> None:
        self.wrapped = wrapped
        self.steps_by_execution = steps_by_execution

    def __getattr__(self, name: str) -> Any:
        return getattr(self.wrapped, name)

    def get_paginator(self, operation_name: str) -> Any:
        if operation_name == "list_pipeline_execution_steps":
            return FakePipelineStepsPaginator(self.steps_by_execution)
        return self.wrapped.get_paginator(operation_name)


def context_with_steps(
    sagemaker_client: Any,
    logs_client: Any,
    execution_arn: str,
) -> AwsContext:
    return AwsContext(
        profile="mock-dev",
        region=REGION,
        sagemaker=SageMakerWithPipelineSteps(sagemaker_client, {execution_arn: pipeline_steps(execution_arn)}),
        logs=logs_client,
    )
