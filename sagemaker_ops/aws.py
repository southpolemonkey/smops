from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import boto3
from botocore.exceptions import BotoCoreError, ClientError


ACTIVE_PROCESSING_STATUSES = ("InProgress", "Stopping")
ACTIVE_PIPELINE_STATUSES = ("Executing", "Stopping")


class AwsCliError(RuntimeError):
    """User-facing error raised by the CLI layer."""


@dataclass(frozen=True)
class AwsContext:
    profile: str
    region: str
    sagemaker: Any
    logs: Any


@dataclass(frozen=True)
class ProcessingJobView:
    profile: str
    region: str
    name: str
    status: str
    creation_time: datetime | None
    last_modified_time: datetime | None
    started_time: datetime | None
    ended_time: datetime | None
    instance_type: str
    instance_count: int | None
    role_arn: str
    failure_reason: str
    arn: str


@dataclass(frozen=True)
class PipelineExecutionView:
    profile: str
    region: str
    pipeline_name: str
    execution_arn: str
    display_name: str
    status: str
    start_time: datetime | None
    last_modified_time: datetime | None


def load_job_spec(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AwsCliError(f"配置文件不存在: {path}")

    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(text)
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:
            raise AwsCliError("读取 YAML 需要安装: pip install 'sagemaker-ops-cli[yaml]'") from exc
        loaded = yaml.safe_load(text)
        if not isinstance(loaded, dict):
            raise AwsCliError("YAML 配置必须是一个对象")
        return loaded
    raise AwsCliError("只支持 .json/.yaml/.yml 配置文件")


def parse_parameters(items: Iterable[str]) -> list[dict[str, str]]:
    parameters: list[dict[str, str]] = []
    for item in items:
        if "=" not in item:
            raise AwsCliError(f"Pipeline 参数必须是 NAME=VALUE 格式: {item}")
        name, value = item.split("=", 1)
        name = name.strip()
        if not name:
            raise AwsCliError(f"Pipeline 参数名不能为空: {item}")
        parameters.append({"Name": name, "Value": value})
    return parameters


def build_contexts(
    profiles: tuple[str, ...],
    region: str | None,
    all_profiles: bool = False,
) -> list[AwsContext]:
    if all_profiles:
        session = boto3.Session()
        names = tuple(session.available_profiles)
        if not names:
            raise AwsCliError("没有找到任何 AWS profile")
    else:
        names = profiles or (None,)

    contexts: list[AwsContext] = []
    for profile in names:
        try:
            session = boto3.Session(profile_name=profile, region_name=region)
            resolved_region = session.region_name or region
            if not resolved_region:
                label = profile or "default/env"
                raise AwsCliError(f"profile {label} 没有配置 region，请传 --region")
            contexts.append(
                AwsContext(
                    profile=profile or session.profile_name or "default/env",
                    region=resolved_region,
                    sagemaker=session.client("sagemaker"),
                    logs=session.client("logs"),
                )
            )
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"创建 AWS session 失败 profile={profile or 'default/env'}: {exc}") from exc
    return contexts


def submit_processing_job(ctx: AwsContext, spec: dict[str, Any]) -> dict[str, Any]:
    try:
        return ctx.sagemaker.create_processing_job(**spec)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"提交 processing job 失败: {exc}") from exc


def start_pipeline_execution(
    ctx: AwsContext,
    pipeline_name: str,
    display_name: str | None,
    parameters: list[dict[str, str]],
    client_request_token: str | None,
) -> dict[str, Any]:
    request: dict[str, Any] = {"PipelineName": pipeline_name}
    if display_name:
        request["PipelineExecutionDisplayName"] = display_name
    if parameters:
        request["PipelineParameters"] = parameters
    if client_request_token:
        request["ClientRequestToken"] = client_request_token

    try:
        return ctx.sagemaker.start_pipeline_execution(**request)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"启动 pipeline 失败: {exc}") from exc


def list_processing_jobs(ctx: AwsContext, max_results: int = 50) -> list[ProcessingJobView]:
    summaries: list[dict[str, Any]] = []
    for status in ACTIVE_PROCESSING_STATUSES:
        paginator = ctx.sagemaker.get_paginator("list_processing_jobs")
        for page in paginator.paginate(
            StatusEquals=status,
            SortBy="CreationTime",
            SortOrder="Descending",
            PaginationConfig={"PageSize": min(max_results, 100), "MaxItems": max_results},
        ):
            summaries.extend(page.get("ProcessingJobSummaries", []))

    jobs: list[ProcessingJobView] = []
    for summary in summaries:
        name = summary["ProcessingJobName"]
        try:
            detail = ctx.sagemaker.describe_processing_job(ProcessingJobName=name)
        except (BotoCoreError, ClientError):
            detail = summary
        cluster = (
            detail.get("ProcessingResources", {})
            .get("ClusterConfig", {})
        )
        jobs.append(
            ProcessingJobView(
                profile=ctx.profile,
                region=ctx.region,
                name=name,
                status=detail.get("ProcessingJobStatus", summary.get("ProcessingJobStatus", "")),
                creation_time=detail.get("CreationTime", summary.get("CreationTime")),
                last_modified_time=detail.get("LastModifiedTime"),
                started_time=detail.get("ProcessingStartTime"),
                ended_time=detail.get("ProcessingEndTime"),
                instance_type=cluster.get("InstanceType", ""),
                instance_count=cluster.get("InstanceCount"),
                role_arn=detail.get("RoleArn", ""),
                failure_reason=detail.get("FailureReason", ""),
                arn=detail.get("ProcessingJobArn", summary.get("ProcessingJobArn", "")),
            )
        )
    return sorted(jobs, key=lambda job: job.creation_time or datetime.min.replace(tzinfo=timezone.utc), reverse=True)


def list_active_pipeline_executions(
    ctx: AwsContext,
    pipeline_name: str | None = None,
    per_pipeline: int = 10,
) -> list[PipelineExecutionView]:
    names = [pipeline_name] if pipeline_name else _list_pipeline_names(ctx)
    executions: list[PipelineExecutionView] = []

    for name in names:
        paginator = ctx.sagemaker.get_paginator("list_pipeline_executions")
        for page in paginator.paginate(
            PipelineName=name,
            SortBy="CreationTime",
            SortOrder="Descending",
            PaginationConfig={"PageSize": min(per_pipeline, 100), "MaxItems": per_pipeline},
        ):
            for summary in page.get("PipelineExecutionSummaries", []):
                status = summary.get("PipelineExecutionStatus", "")
                if status not in ACTIVE_PIPELINE_STATUSES:
                    continue
                executions.append(
                    PipelineExecutionView(
                        profile=ctx.profile,
                        region=ctx.region,
                        pipeline_name=name,
                        execution_arn=summary.get("PipelineExecutionArn", ""),
                        display_name=summary.get("PipelineExecutionDisplayName", ""),
                        status=status,
                        start_time=summary.get("StartTime"),
                        last_modified_time=summary.get("LastModifiedTime"),
                    )
                )
    return sorted(executions, key=lambda item: item.start_time or datetime.min.replace(tzinfo=timezone.utc), reverse=True)


def list_pipeline_steps(ctx: AwsContext, execution_arn: str) -> list[dict[str, Any]]:
    paginator = ctx.sagemaker.get_paginator("list_pipeline_execution_steps")
    steps: list[dict[str, Any]] = []
    try:
        for page in paginator.paginate(PipelineExecutionArn=execution_arn):
            steps.extend(page.get("PipelineExecutionSteps", []))
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 pipeline steps 失败: {exc}") from exc
    return sorted(steps, key=lambda step: step.get("StartTime") or datetime.min.replace(tzinfo=timezone.utc))


def describe_pipeline_execution(ctx: AwsContext, execution_arn: str) -> dict[str, Any]:
    try:
        return ctx.sagemaker.describe_pipeline_execution(PipelineExecutionArn=execution_arn)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 pipeline execution 失败: {exc}") from exc


def tail_step_logs(ctx: AwsContext, step: dict[str, Any], limit: int = 80) -> list[str]:
    source = infer_log_source(step)
    if source is None:
        return []
    log_group, stream_prefix = source
    return tail_cloudwatch_logs(ctx, log_group, stream_prefix, limit=limit)


def tail_cloudwatch_logs(
    ctx: AwsContext,
    log_group: str,
    stream_prefix: str,
    limit: int = 80,
) -> list[str]:
    try:
        streams = ctx.logs.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=stream_prefix,
            limit=5,
        ).get("logStreams", [])
    except ctx.logs.exceptions.ResourceNotFoundException:
        return [f"没有找到日志组: {log_group}"]
    except (BotoCoreError, ClientError) as exc:
        return [f"读取日志流失败: {exc}"]

    streams = sorted(streams, key=lambda stream: stream.get("lastEventTimestamp", 0), reverse=True)
    lines: list[str] = []
    for stream in streams:
        stream_name = stream["logStreamName"]
        try:
            events = ctx.logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream_name,
                limit=limit,
                startFromHead=False,
            ).get("events", [])
        except (BotoCoreError, ClientError) as exc:
            lines.append(f"[{stream_name}] 读取失败: {exc}")
            continue
        for event in events[-limit:]:
            timestamp = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
            lines.append(f"{timestamp:%Y-%m-%d %H:%M:%S}Z {event.get('message', '').rstrip()}")
    return lines[-limit:]


def infer_log_source(step: dict[str, Any]) -> tuple[str, str] | None:
    metadata = step.get("Metadata") or {}
    sources = (
        ("ProcessingJob", "/aws/sagemaker/ProcessingJobs"),
        ("TrainingJob", "/aws/sagemaker/TrainingJobs"),
        ("TransformJob", "/aws/sagemaker/TransformJobs"),
    )
    for key, log_group in sources:
        payload = metadata.get(key)
        if not isinstance(payload, dict):
            continue
        arn = payload.get("Arn")
        if arn:
            return log_group, arn.rsplit("/", 1)[-1]
    return None


def format_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def format_duration(start: datetime | None, end: datetime | None = None) -> str:
    if start is None:
        return ""
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    finish = end or datetime.now(timezone.utc)
    if finish.tzinfo is None:
        finish = finish.replace(tzinfo=timezone.utc)
    seconds = max(0, int((finish - start).total_seconds()))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _list_pipeline_names(ctx: AwsContext) -> list[str]:
    paginator = ctx.sagemaker.get_paginator("list_pipelines")
    names: list[str] = []
    for page in paginator.paginate(SortBy="CreationTime", SortOrder="Descending"):
        names.extend(item["PipelineName"] for item in page.get("PipelineSummaries", []))
    return names

