from __future__ import annotations

import base64
import binascii
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError


ACTIVE_PROCESSING_STATUSES = ("InProgress", "Stopping")
ACTIVE_PIPELINE_STATUSES = ("Executing", "Stopping")
TERMINAL_PROCESSING_STATUSES = ("Completed", "Failed", "Stopped")
TERMINAL_PIPELINE_STATUSES = ("Succeeded", "Failed", "Stopped")


class AwsCliError(RuntimeError):
    """User-facing error raised by the CLI layer."""


@dataclass(frozen=True)
class AwsContext:
    profile: str
    region: str
    sagemaker: Any
    logs: Any
    ecs: Any = None
    mwaa: Any = None


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
class ProcessingJobsPage:
    jobs: list[ProcessingJobView]
    next_token: str | None


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


@dataclass(frozen=True)
class PipelineExecutionsPage:
    executions: list[PipelineExecutionView]
    next_token: str | None


@dataclass(frozen=True)
class EcsClusterView:
    profile: str
    region: str
    cluster_arn: str
    cluster_name: str
    status: str
    running_tasks: int
    pending_tasks: int
    active_services: int


@dataclass(frozen=True)
class EcsServiceView:
    profile: str
    region: str
    cluster: str
    service_arn: str
    service_name: str
    status: str
    desired_count: int
    running_count: int
    pending_count: int
    task_definition: str


@dataclass(frozen=True)
class EcsTaskView:
    profile: str
    region: str
    cluster: str
    task_arn: str
    task_id: str
    task_definition_arn: str
    last_status: str
    desired_status: str
    launch_type: str
    started_at: datetime | None
    stopped_at: datetime | None
    stopped_reason: str
    containers: list[dict[str, Any]]


@dataclass(frozen=True)
class EcsLogSource:
    container_name: str
    log_group: str
    stream_prefix: str
    log_stream: str


@dataclass(frozen=True)
class EcsTaskHistoryEntry:
    task_id: str
    log_stream: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: float


@dataclass(frozen=True)
class MwaaEnvironmentView:
    profile: str
    region: str
    name: str
    status: str
    airflow_version: str
    webserver_url: str
    schedulers: int | None


@dataclass(frozen=True)
class AirflowDagView:
    profile: str
    region: str
    environment: str
    dag_id: str
    is_paused: bool
    is_active: bool
    schedule_interval: str
    owners: list[str]
    tags: list[str]
    description: str


@dataclass(frozen=True)
class AirflowDagRunView:
    profile: str
    region: str
    environment: str
    dag_id: str
    dag_run_id: str
    state: str
    run_type: str
    logical_date: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    external_trigger: bool


@dataclass(frozen=True)
class AirflowTaskInstanceView:
    profile: str
    region: str
    environment: str
    dag_id: str
    dag_run_id: str
    task_id: str
    state: str
    try_number: int | None
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None


@dataclass(frozen=True)
class AirflowPoolView:
    profile: str
    region: str
    environment: str
    name: str
    slots: int
    occupied_slots: int
    running_slots: int
    queued_slots: int
    open_slots: int


def list_ecs_task_history(
    ctx: AwsContext,
    log_group: str,
    stream_prefix: str,
    limit: int = 20,
) -> list[EcsTaskHistoryEntry]:
    """Return historical ECS task runs by inspecting CloudWatch log streams.

    The ECS API only retains stopped tasks for ~1 hour; log streams persist
    for the lifetime of the log group, making this the reliable way to enumerate
    past runs. Each stream's firstEventTimestamp / lastEventTimestamp proxy the
    task start and end times.
    """
    # AWS does not allow orderBy=LastEventTime with a logStreamNamePrefix.
    # Without a prefix: use LastEventTime order and stop early (most-recent first).
    # With a prefix: must use LogStreamName order — exhaust all matching pages then
    # sort client-side (task IDs are random UUIDs that sort in arbitrary order).
    try:
        paginator = ctx.logs.get_paginator("describe_log_streams")
        streams: list[dict[str, Any]] = []
        if stream_prefix:
            kwargs: dict[str, Any] = {
                "logGroupName": log_group,
                "logStreamNamePrefix": stream_prefix,
                "orderBy": "LogStreamName",
                "descending": True,
            }
            for page in paginator.paginate(**kwargs):
                streams.extend(page.get("logStreams", []))
        else:
            kwargs = {"logGroupName": log_group, "orderBy": "LastEventTime", "descending": True}
            for page in paginator.paginate(**kwargs, PaginationConfig={"MaxItems": limit}):
                streams.extend(page.get("logStreams", []))
    except ctx.logs.exceptions.ResourceNotFoundException:
        raise AwsCliError(f"日志组不存在: {log_group}")
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取日志流历史失败: {exc}") from exc

    streams.sort(key=lambda s: s.get("lastEventTimestamp", 0), reverse=True)

    entries: list[EcsTaskHistoryEntry] = []
    for stream in streams[:limit]:
        first_ms = stream.get("firstEventTimestamp")
        last_ms = stream.get("lastEventTimestamp")
        if not first_ms or not last_ms:
            continue
        stream_name = stream["logStreamName"]
        task_id = stream_name.rsplit("/", 1)[-1]
        started = datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc)
        ended = datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc)
        entries.append(EcsTaskHistoryEntry(
            task_id=task_id,
            log_stream=stream_name,
            started_at=started,
            ended_at=ended,
            duration_seconds=(last_ms - first_ms) / 1000,
        ))
    return entries


def list_ecs_clusters(ctx: AwsContext) -> list[EcsClusterView]:
    arns = _paginate_arns(ctx.ecs, "list_clusters", "clusterArns")
    if not arns:
        return []
    clusters: list[EcsClusterView] = []
    for chunk in _chunks(arns, 100):
        try:
            response = ctx.ecs.describe_clusters(clusters=chunk)
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"读取 ECS clusters 失败: {exc}") from exc
        for cluster in response.get("clusters", []):
            clusters.append(
                EcsClusterView(
                    profile=ctx.profile,
                    region=ctx.region,
                    cluster_arn=cluster.get("clusterArn", ""),
                    cluster_name=cluster.get("clusterName", ""),
                    status=cluster.get("status", ""),
                    running_tasks=cluster.get("runningTasksCount", 0),
                    pending_tasks=cluster.get("pendingTasksCount", 0),
                    active_services=cluster.get("activeServicesCount", 0),
                )
            )
    return sorted(clusters, key=lambda item: item.cluster_name)


def list_ecs_services(ctx: AwsContext, cluster: str) -> list[EcsServiceView]:
    arns = _paginate_arns(ctx.ecs, "list_services", "serviceArns", cluster=cluster)
    if not arns:
        return []
    services: list[EcsServiceView] = []
    for chunk in _chunks(arns, 10):
        try:
            response = ctx.ecs.describe_services(cluster=cluster, services=chunk)
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"读取 ECS services 失败 cluster={cluster}: {exc}") from exc
        for service in response.get("services", []):
            services.append(
                EcsServiceView(
                    profile=ctx.profile,
                    region=ctx.region,
                    cluster=cluster,
                    service_arn=service.get("serviceArn", ""),
                    service_name=service.get("serviceName", ""),
                    status=service.get("status", ""),
                    desired_count=service.get("desiredCount", 0),
                    running_count=service.get("runningCount", 0),
                    pending_count=service.get("pendingCount", 0),
                    task_definition=service.get("taskDefinition", ""),
                )
            )
    return sorted(services, key=lambda item: item.service_name)


def list_ecs_tasks(
    ctx: AwsContext,
    cluster: str,
    service: str | None = None,
    desired_status: str = "RUNNING",
) -> list[EcsTaskView]:
    request = {"cluster": cluster, "desiredStatus": desired_status.upper()}
    if service:
        request["serviceName"] = service
    arns = _paginate_arns(ctx.ecs, "list_tasks", "taskArns", **request)
    if not arns:
        return []
    return describe_ecs_tasks(ctx, cluster, arns)


def describe_ecs_task(ctx: AwsContext, cluster: str, task: str) -> EcsTaskView:
    tasks = describe_ecs_tasks(ctx, cluster, [task])
    if not tasks:
        raise AwsCliError(f"没有找到 ECS task: {task}")
    return tasks[0]


def describe_ecs_tasks(ctx: AwsContext, cluster: str, tasks: list[str]) -> list[EcsTaskView]:
    views: list[EcsTaskView] = []
    for chunk in _chunks(tasks, 100):
        try:
            response = ctx.ecs.describe_tasks(cluster=cluster, tasks=chunk)
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"读取 ECS tasks 失败 cluster={cluster}: {exc}") from exc
        failures = response.get("failures", [])
        if failures and not response.get("tasks"):
            reason = failures[0].get("reason", "unknown")
            arn = failures[0].get("arn", "")
            raise AwsCliError(f"读取 ECS task 失败 task={arn or chunk[0]}: {reason}")
        for task in response.get("tasks", []):
            views.append(_ecs_task_view_from_detail(ctx, cluster, task))
    return sorted(views, key=lambda item: item.started_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)


def tail_ecs_task_logs(
    ctx: AwsContext,
    cluster: str,
    task: str,
    container: str | None = None,
    limit: int = 80,
) -> list[str]:
    task_view = describe_ecs_task(ctx, cluster, task)
    source = resolve_ecs_task_log_source(ctx, task_view, container)
    return tail_cloudwatch_logs(ctx, source.log_group, source.log_stream, limit=limit)


def resolve_ecs_task_log_source(ctx: AwsContext, task: EcsTaskView, container: str | None = None) -> EcsLogSource:
    try:
        response = ctx.ecs.describe_task_definition(taskDefinition=task.task_definition_arn)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 ECS task definition 失败: {exc}") from exc
    definitions = response.get("taskDefinition", {}).get("containerDefinitions", [])
    awslogs = [definition for definition in definitions if _is_awslogs_container(definition)]
    if container:
        awslogs = [definition for definition in awslogs if definition.get("name") == container]
        if not awslogs:
            raise AwsCliError(f"container {container} 没有配置 awslogs log driver")
    elif len(awslogs) > 1:
        names = ", ".join(definition.get("name", "") for definition in awslogs)
        raise AwsCliError(f"Task 有多个 awslogs containers，请传 --container。可选: {names}")
    elif not awslogs:
        raise AwsCliError("Task definition 没有配置 awslogs log driver")

    definition = awslogs[0]
    options = definition.get("logConfiguration", {}).get("options", {})
    log_group = options.get("awslogs-group")
    stream_prefix = options.get("awslogs-stream-prefix")
    container_name = definition.get("name", "")
    if not log_group or not stream_prefix or not container_name:
        raise AwsCliError("awslogs 配置缺少 awslogs-group、awslogs-stream-prefix 或 container name")
    task_id = task.task_id
    log_stream = f"{stream_prefix}/{container_name}/{task_id}"
    return EcsLogSource(
        container_name=container_name,
        log_group=log_group,
        stream_prefix=stream_prefix,
        log_stream=log_stream,
    )


def _ecs_task_view_from_detail(ctx: AwsContext, cluster: str, task: dict[str, Any]) -> EcsTaskView:
    task_arn = task.get("taskArn", "")
    return EcsTaskView(
        profile=ctx.profile,
        region=ctx.region,
        cluster=cluster,
        task_arn=task_arn,
        task_id=_ecs_task_id(task_arn),
        task_definition_arn=task.get("taskDefinitionArn", ""),
        last_status=task.get("lastStatus", ""),
        desired_status=task.get("desiredStatus", ""),
        launch_type=task.get("launchType", ""),
        started_at=task.get("startedAt"),
        stopped_at=task.get("stoppedAt"),
        stopped_reason=task.get("stoppedReason", ""),
        containers=[
            {
                "name": container.get("name", ""),
                "last_status": container.get("lastStatus", ""),
                "exit_code": container.get("exitCode"),
                "reason": container.get("reason", ""),
                "runtime_id": container.get("runtimeId", ""),
            }
            for container in task.get("containers", [])
        ],
    )


def _paginate_arns(client: Any, operation_name: str, result_key: str, **kwargs: Any) -> list[str]:
    try:
        paginator = client.get_paginator(operation_name)
        items: list[str] = []
        for page in paginator.paginate(**kwargs):
            items.extend(page.get(result_key, []))
        return items
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 ECS {operation_name} 失败: {exc}") from exc


def _is_awslogs_container(definition: dict[str, Any]) -> bool:
    log_config = definition.get("logConfiguration") or {}
    return log_config.get("logDriver") == "awslogs"


def _ecs_task_id(task_arn: str) -> str:
    return task_arn.rsplit("/", 1)[-1] if task_arn else ""


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


# --- Amazon MWAA / Apache Airflow ------------------------------------------
#
# MWAA's boto3 client only exposes list/get environment and create_web_login_token.
# DAG, run, task, and pool data live behind the Airflow REST API on the private
# web server. Access flow: create a short-lived web login token, POST it to
# /aws_mwaa/login to obtain a session cookie, then call /api/v1/... with the
# cookie. Tokens expire quickly, so a fresh session is created per call.

AIRFLOW_REQUEST_TIMEOUT = 30


def list_mwaa_environments(ctx: AwsContext) -> list[MwaaEnvironmentView]:
    try:
        paginator = ctx.mwaa.get_paginator("list_environments")
        names: list[str] = []
        for page in paginator.paginate():
            names.extend(page.get("Environments", []))
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 MWAA environments 失败: {exc}") from exc

    environments: list[MwaaEnvironmentView] = []
    for name in names:
        try:
            detail = ctx.mwaa.get_environment(Name=name).get("Environment", {})
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"读取 MWAA environment 失败 name={name}: {exc}") from exc
        environments.append(
            MwaaEnvironmentView(
                profile=ctx.profile,
                region=ctx.region,
                name=detail.get("Name", name),
                status=detail.get("Status", ""),
                airflow_version=detail.get("AirflowVersion", ""),
                webserver_url=detail.get("WebserverUrl", ""),
                schedulers=detail.get("Schedulers"),
            )
        )
    return sorted(environments, key=lambda item: item.name)


def _airflow_session(ctx: AwsContext, environment: str) -> tuple[requests.Session, str]:
    """Create an authenticated requests.Session for the Airflow web server.

    Exchanges a fresh MWAA web login token for a session cookie. Returns the
    session (carrying the cookie) plus the web server hostname.
    """
    try:
        response = ctx.mwaa.create_web_login_token(Name=environment)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"创建 MWAA web login token 失败 env={environment}: {exc}") from exc

    token = response.get("WebToken")
    host = response.get("WebServerHostname")
    if not token or not host:
        raise AwsCliError(f"MWAA 未返回 web login token 或 hostname: env={environment}")

    session = requests.Session()
    try:
        login = session.post(
            f"https://{host}/aws_mwaa/login",
            data={"token": token},
            timeout=AIRFLOW_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        login.raise_for_status()
    except requests.RequestException as exc:
        raise AwsCliError(f"登录 Airflow web server 失败 env={environment}: {exc}") from exc
    return session, host


def _airflow_get(session: requests.Session, host: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return _airflow_request(session, host, "GET", path, params=params)


def _airflow_request(
    session: requests.Session,
    host: str,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"https://{host}/api/v1/{path.lstrip('/')}"
    try:
        response = session.request(
            method,
            url,
            params=params,
            json=json_body,
            timeout=AIRFLOW_REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise AwsCliError(f"调用 Airflow API 失败 {method} {path}: {exc}") from exc

    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("detail") or payload.get("title") or ""
        except ValueError:
            detail = response.text[:200]
        raise AwsCliError(f"Airflow API {method} {path} 返回 {response.status_code}: {detail}")

    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError as exc:
        raise AwsCliError(f"Airflow API {method} {path} 返回非 JSON 响应") from exc


def list_airflow_dags(
    ctx: AwsContext,
    environment: str,
    name_pattern: str | None = None,
    limit: int = 100,
) -> list[AirflowDagView]:
    session, host = _airflow_session(ctx, environment)
    params: dict[str, Any] = {"limit": max(1, min(limit, 100))}
    if name_pattern:
        params["dag_id_pattern"] = name_pattern
    payload = _airflow_get(session, host, "dags", params=params)
    dags = [
        AirflowDagView(
            profile=ctx.profile,
            region=ctx.region,
            environment=environment,
            dag_id=dag.get("dag_id", ""),
            is_paused=bool(dag.get("is_paused", False)),
            is_active=bool(dag.get("is_active", False)),
            schedule_interval=_format_schedule_interval(dag.get("schedule_interval")),
            owners=list(dag.get("owners", []) or []),
            tags=[tag.get("name", "") for tag in dag.get("tags", []) or []],
            description=dag.get("description") or "",
        )
        for dag in payload.get("dags", [])
    ]
    return sorted(dags, key=lambda item: item.dag_id)


def list_airflow_dag_runs(
    ctx: AwsContext,
    environment: str,
    dag_id: str,
    limit: int = 10,
) -> list[AirflowDagRunView]:
    session, host = _airflow_session(ctx, environment)
    params = {"limit": max(1, min(limit, 100)), "order_by": "-execution_date"}
    payload = _airflow_get(session, host, f"dags/{dag_id}/dagRuns", params=params)
    return [_airflow_dag_run_view(ctx, environment, dag_id, run) for run in payload.get("dag_runs", [])]


def list_airflow_task_instances(
    ctx: AwsContext,
    environment: str,
    dag_id: str,
    dag_run_id: str,
) -> list[AirflowTaskInstanceView]:
    session, host = _airflow_session(ctx, environment)
    payload = _airflow_get(session, host, f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
    instances = [
        AirflowTaskInstanceView(
            profile=ctx.profile,
            region=ctx.region,
            environment=environment,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=instance.get("task_id", ""),
            state=instance.get("state") or "",
            try_number=instance.get("try_number"),
            start_date=_parse_airflow_dt(instance.get("start_date")),
            end_date=_parse_airflow_dt(instance.get("end_date")),
            duration=instance.get("duration"),
        )
        for instance in payload.get("task_instances", [])
    ]
    return sorted(instances, key=lambda item: item.start_date or datetime.min.replace(tzinfo=timezone.utc))


def list_airflow_pools(ctx: AwsContext, environment: str) -> list[AirflowPoolView]:
    session, host = _airflow_session(ctx, environment)
    payload = _airflow_get(session, host, "pools")
    pools = [
        AirflowPoolView(
            profile=ctx.profile,
            region=ctx.region,
            environment=environment,
            name=pool.get("name", ""),
            slots=pool.get("slots", 0),
            occupied_slots=pool.get("occupied_slots", 0),
            running_slots=pool.get("running_slots", 0),
            queued_slots=pool.get("queued_slots", 0),
            open_slots=pool.get("open_slots", 0),
        )
        for pool in payload.get("pools", [])
    ]
    return sorted(pools, key=lambda item: item.name)


def trigger_airflow_dag(
    ctx: AwsContext,
    environment: str,
    dag_id: str,
    conf: dict[str, Any] | None = None,
    logical_date: str | None = None,
) -> AirflowDagRunView:
    session, host = _airflow_session(ctx, environment)
    body: dict[str, Any] = {}
    if conf is not None:
        body["conf"] = conf
    if logical_date:
        body["logical_date"] = logical_date
    payload = _airflow_request(session, host, "POST", f"dags/{dag_id}/dagRuns", json_body=body)
    return _airflow_dag_run_view(ctx, environment, dag_id, payload)


def _airflow_dag_run_view(
    ctx: AwsContext,
    environment: str,
    dag_id: str,
    run: dict[str, Any],
) -> AirflowDagRunView:
    return AirflowDagRunView(
        profile=ctx.profile,
        region=ctx.region,
        environment=environment,
        dag_id=run.get("dag_id", dag_id),
        dag_run_id=run.get("dag_run_id", ""),
        state=run.get("state") or "",
        run_type=run.get("run_type", ""),
        logical_date=_parse_airflow_dt(run.get("logical_date")),
        start_date=_parse_airflow_dt(run.get("start_date")),
        end_date=_parse_airflow_dt(run.get("end_date")),
        external_trigger=bool(run.get("external_trigger", False)),
    )


def _format_schedule_interval(schedule_interval: Any) -> str:
    if schedule_interval is None:
        return ""
    if isinstance(schedule_interval, dict):
        # Airflow returns e.g. {"__type": "CronExpression", "value": "0 * * * *"}.
        return str(schedule_interval.get("value", schedule_interval))
    return str(schedule_interval)


def _parse_airflow_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


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


def available_profiles() -> list[str]:
    try:
        return list(boto3.Session().available_profiles)
    except BotoCoreError as exc:
        raise AwsCliError(f"读取 AWS profiles 失败: {exc}") from exc


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
                    ecs=session.client("ecs"),
                    mwaa=session.client("mwaa"),
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


def describe_processing_job(ctx: AwsContext, job_name: str) -> ProcessingJobView:
    try:
        detail = ctx.sagemaker.describe_processing_job(ProcessingJobName=job_name)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 processing job 失败 job={job_name}: {exc}") from exc
    return _processing_job_view_from_detail(ctx, detail)


def wait_processing_job(
    ctx: AwsContext,
    job_name: str,
    timeout_seconds: int = 3600,
    poll_seconds: int = 30,
) -> ProcessingJobView:
    deadline = time.monotonic() + max(0, timeout_seconds)
    poll_seconds = max(1, poll_seconds)

    while True:
        job = describe_processing_job(ctx, job_name)
        if job.status in TERMINAL_PROCESSING_STATUSES:
            return job
        if time.monotonic() >= deadline:
            raise AwsCliError(f"等待 processing job 超时 job={job_name} status={job.status}")
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))


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
    return list_processing_jobs_page(ctx, page_size=max_results).jobs


def list_processing_jobs_page(
    ctx: AwsContext,
    page_size: int = 20,
    next_token: str | None = None,
) -> ProcessingJobsPage:
    page_size = max(1, min(page_size, 100))
    status_index, aws_next_token = _decode_processing_jobs_token(next_token)
    summaries: list[dict[str, Any]] = []
    output_next_token: str | None = None

    while len(summaries) < page_size and status_index < len(ACTIVE_PROCESSING_STATUSES):
        status = ACTIVE_PROCESSING_STATUSES[status_index]
        request: dict[str, Any] = {
            "StatusEquals": status,
            "SortBy": "CreationTime",
            "SortOrder": "Descending",
            "MaxResults": min(100, page_size - len(summaries)),
        }
        if aws_next_token:
            request["NextToken"] = aws_next_token

        try:
            response = ctx.sagemaker.list_processing_jobs(**request)
        except (BotoCoreError, ClientError) as exc:
            raise AwsCliError(f"读取 processing jobs 失败: {exc}") from exc

        summaries.extend(response.get("ProcessingJobSummaries", []))
        aws_next_token = response.get("NextToken")
        if aws_next_token:
            output_next_token = _encode_processing_jobs_token(status_index, aws_next_token)
            break
        status_index += 1

    jobs = [_processing_job_view_from_summary(ctx, summary) for summary in summaries]
    return ProcessingJobsPage(
        jobs=sorted(jobs, key=lambda job: job.creation_time or datetime.min.replace(tzinfo=timezone.utc), reverse=True),
        next_token=output_next_token,
    )


def _processing_job_view_from_summary(ctx: AwsContext, summary: dict[str, Any]) -> ProcessingJobView:
    name = summary["ProcessingJobName"]
    try:
        detail = ctx.sagemaker.describe_processing_job(ProcessingJobName=name)
    except (BotoCoreError, ClientError):
        detail = summary
    return _processing_job_view_from_detail(ctx, detail, fallback=summary)


def _processing_job_view_from_detail(
    ctx: AwsContext,
    detail: dict[str, Any],
    fallback: dict[str, Any] | None = None,
) -> ProcessingJobView:
    fallback = fallback or {}
    cluster = detail.get("ProcessingResources", {}).get("ClusterConfig", {})
    return ProcessingJobView(
        profile=ctx.profile,
        region=ctx.region,
        name=detail.get("ProcessingJobName", fallback.get("ProcessingJobName", "")),
        status=detail.get("ProcessingJobStatus", fallback.get("ProcessingJobStatus", "")),
        creation_time=detail.get("CreationTime", fallback.get("CreationTime")),
        last_modified_time=detail.get("LastModifiedTime"),
        started_time=detail.get("ProcessingStartTime"),
        ended_time=detail.get("ProcessingEndTime"),
        instance_type=cluster.get("InstanceType", ""),
        instance_count=cluster.get("InstanceCount"),
        role_arn=detail.get("RoleArn", ""),
        failure_reason=detail.get("FailureReason", ""),
        arn=detail.get("ProcessingJobArn", fallback.get("ProcessingJobArn", "")),
    )


def _encode_processing_jobs_token(status_index: int, aws_next_token: str) -> str:
    payload = json.dumps({"status_index": status_index, "aws_next_token": aws_next_token}).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_processing_jobs_token(next_token: str | None) -> tuple[int, str | None]:
    if not next_token:
        return 0, None
    try:
        decoded = base64.urlsafe_b64decode(next_token.encode("ascii"))
        payload = json.loads(decoded.decode("utf-8"))
        status_index = int(payload.get("status_index", 0))
        aws_next_token = payload.get("aws_next_token")
    except (binascii.Error, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise AwsCliError("processing jobs next token 无效") from exc
    if status_index < 0 or status_index >= len(ACTIVE_PROCESSING_STATUSES) or not isinstance(aws_next_token, str):
        raise AwsCliError("processing jobs next token 无效")
    return status_index, aws_next_token


def list_active_pipeline_executions(
    ctx: AwsContext,
    pipeline_name: str | None = None,
    per_pipeline: int = 10,
    recent_hours: int = 3,
) -> list[PipelineExecutionView]:
    return list_pipeline_executions_page(
        ctx,
        pipeline_name=pipeline_name,
        per_pipeline=per_pipeline,
        recent_hours=recent_hours,
    ).executions


def list_pipeline_executions_page(
    ctx: AwsContext,
    pipeline_name: str | None = None,
    per_pipeline: int = 10,
    recent_hours: int = 3,
    pipeline_page_size: int = 10,
    next_token: str | None = None,
) -> PipelineExecutionsPage:
    names, output_next_token = _list_pipeline_names_page(
        ctx,
        pipeline_name=pipeline_name,
        page_size=pipeline_page_size,
        next_token=next_token,
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=recent_hours)
    executions = _list_recent_pipeline_executions(ctx, names, per_pipeline, cutoff)

    return PipelineExecutionsPage(
        executions=sorted(
            executions,
            key=lambda item: item.last_modified_time or item.start_time or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        ),
        next_token=output_next_token,
    )


def _list_recent_pipeline_executions(
    ctx: AwsContext,
    pipeline_names: list[str],
    per_pipeline: int,
    cutoff: datetime,
) -> list[PipelineExecutionView]:
    if not pipeline_names:
        return []
    if len(pipeline_names) == 1:
        return _list_recent_pipeline_executions_for_name(ctx, pipeline_names[0], per_pipeline, cutoff)

    executions: list[PipelineExecutionView] = []
    workers = min(8, len(pipeline_names))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_list_recent_pipeline_executions_for_name, ctx, name, per_pipeline, cutoff)
            for name in pipeline_names
        ]
        for future in as_completed(futures):
            executions.extend(future.result())
    return executions


def _list_recent_pipeline_executions_for_name(
    ctx: AwsContext,
    pipeline_name: str,
    per_pipeline: int,
    cutoff: datetime,
) -> list[PipelineExecutionView]:
    request = {
        "PipelineName": pipeline_name,
        "SortBy": "CreationTime",
        "SortOrder": "Descending",
        "MaxResults": max(1, min(per_pipeline, 100)),
    }
    try:
        response = ctx.sagemaker.list_pipeline_executions(**request)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 pipeline executions 失败 pipeline={pipeline_name}: {exc}") from exc

    executions: list[PipelineExecutionView] = []
    for summary in response.get("PipelineExecutionSummaries", []):
        status = summary.get("PipelineExecutionStatus", "")
        execution_arn = summary.get("PipelineExecutionArn", "")
        start_time = summary.get("StartTime")
        last_modified_time = summary.get("LastModifiedTime")
        if not _should_show_pipeline_execution(status, start_time, last_modified_time, cutoff):
            continue
        executions.append(
            PipelineExecutionView(
                profile=ctx.profile,
                region=ctx.region,
                pipeline_name=pipeline_name,
                execution_arn=execution_arn,
                display_name=summary.get("PipelineExecutionDisplayName", ""),
                status=status,
                start_time=start_time,
                last_modified_time=last_modified_time,
            )
        )
    return executions


def _list_pipeline_names_page(
    ctx: AwsContext,
    pipeline_name: str | None,
    page_size: int,
    next_token: str | None,
) -> tuple[list[str], str | None]:
    if pipeline_name:
        if next_token:
            raise AwsCliError("指定 --name 时不支持 pipeline next token")
        return [pipeline_name], None

    request: dict[str, Any] = {
        "SortBy": "CreationTime",
        "SortOrder": "Descending",
        "MaxResults": max(1, min(page_size, 100)),
    }
    if next_token:
        request["NextToken"] = next_token
    try:
        response = ctx.sagemaker.list_pipelines(**request)
    except (BotoCoreError, ClientError) as exc:
        raise AwsCliError(f"读取 pipelines 失败: {exc}") from exc

    names = [item["PipelineName"] for item in response.get("PipelineSummaries", [])]
    return names, response.get("NextToken")


def _describe_pipeline_execution_safely(ctx: AwsContext, execution_arn: str) -> dict[str, Any]:
    try:
        return ctx.sagemaker.describe_pipeline_execution(PipelineExecutionArn=execution_arn)
    except (BotoCoreError, ClientError):
        return {}


def _should_show_pipeline_execution(
    status: str,
    start_time: datetime | None,
    last_modified_time: datetime | None,
    cutoff: datetime,
) -> bool:
    if status in ACTIVE_PIPELINE_STATUSES:
        return True
    recent_at = last_modified_time or start_time
    if recent_at is None:
        return False
    if recent_at.tzinfo is None:
        recent_at = recent_at.replace(tzinfo=timezone.utc)
    return recent_at >= cutoff


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


def wait_pipeline_execution(
    ctx: AwsContext,
    execution_arn: str,
    timeout_seconds: int = 3600,
    poll_seconds: int = 30,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0, timeout_seconds)
    poll_seconds = max(1, poll_seconds)

    while True:
        detail = describe_pipeline_execution(ctx, execution_arn)
        status = detail.get("PipelineExecutionStatus", "")
        if status in TERMINAL_PIPELINE_STATUSES:
            return detail
        if time.monotonic() >= deadline:
            raise AwsCliError(f"等待 pipeline execution 超时 execution={execution_arn} status={status}")
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))


def inspect_pipeline_execution(ctx: AwsContext, execution_arn: str) -> dict[str, Any]:
    detail = describe_pipeline_execution(ctx, execution_arn)
    detail = {
        **detail,
        "PipelineExecutionArn": detail.get("PipelineExecutionArn") or execution_arn,
        "PipelineName": detail.get("PipelineName") or _pipeline_name_from_execution_arn(execution_arn),
    }
    steps = list_pipeline_steps(ctx, execution_arn)
    failed_steps = [step for step in steps if step.get("StepStatus") == "Failed"]
    return {
        "profile": ctx.profile,
        "region": ctx.region,
        "execution": detail,
        "steps": steps,
        "failed_steps": failed_steps,
    }


def diagnose_pipeline_execution(
    ctx: AwsContext,
    execution_arn: str,
    log_limit: int = 80,
) -> dict[str, Any]:
    inspection = inspect_pipeline_execution(ctx, execution_arn)
    failed_steps = inspection["failed_steps"]
    failed_step = failed_steps[0] if failed_steps else None
    log_source = infer_log_source(failed_step) if failed_step else None
    log_tail = tail_step_logs(ctx, failed_step, limit=log_limit) if failed_step else []
    job_type = None
    job_name = None
    if log_source and failed_step:
        job_type = _step_job_type(failed_step)
        job_name = log_source[1]

    return {
        **inspection,
        "failed_step": failed_step,
        "job_type": job_type,
        "job_name": job_name,
        "log_group": log_source[0] if log_source else None,
        "log_stream_prefix": log_source[1] if log_source else None,
        "log_tail": log_tail,
        "next_actions": _diagnostic_next_actions(execution_arn, failed_step),
    }


def tail_step_logs(ctx: AwsContext, step: dict[str, Any], limit: int = 80) -> list[str]:
    source = infer_log_source(step)
    if source is None:
        return []
    log_group, stream_prefix = source
    return tail_cloudwatch_logs(ctx, log_group, stream_prefix, limit=limit)


def tail_processing_job_logs(ctx: AwsContext, job_name: str, limit: int = 80) -> list[str]:
    return tail_cloudwatch_logs(ctx, "/aws/sagemaker/ProcessingJobs", job_name, limit=limit)


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


def _step_job_type(step: dict[str, Any]) -> str | None:
    metadata = step.get("Metadata") or {}
    for key in ("ProcessingJob", "TrainingJob", "TransformJob"):
        if isinstance(metadata.get(key), dict):
            return key
    return None


def _pipeline_name_from_execution_arn(execution_arn: str) -> str:
    marker = ":pipeline/"
    if marker not in execution_arn:
        return ""
    tail = execution_arn.split(marker, 1)[1]
    return tail.split("/", 1)[0]


def _diagnostic_next_actions(execution_arn: str, failed_step: dict[str, Any] | None) -> list[dict[str, str]]:
    actions = [
        {
            "type": "inspect",
            "command": f"smops pipeline inspect --execution-arn {execution_arn} --json",
        }
    ]
    if failed_step:
        actions.append(
            {
                "type": "diagnose",
                "command": f"smops pipeline diagnose --execution-arn {execution_arn} --json",
            }
        )
    return actions


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

