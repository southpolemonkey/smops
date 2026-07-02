from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from sagemaker_ops import __version__
from sagemaker_ops.config import config_path, get_default_region, load_config, resolve_region, set_default_region
from sagemaker_ops.aws import (
    AwsCliError,
    build_contexts,
    describe_ecs_task,
    describe_pipeline_execution,
    describe_processing_job,
    diagnose_pipeline_execution,
    format_dt,
    format_duration,
    inspect_pipeline_execution,
    list_ecs_clusters,
    list_ecs_services,
    list_ecs_tasks,
    list_pipeline_executions_page,
    list_pipeline_steps,
    list_processing_jobs_page,
    load_job_spec,
    parse_parameters,
    start_pipeline_execution,
    submit_processing_job,
    tail_ecs_task_logs,
    wait_pipeline_execution,
    wait_processing_job,
)
from sagemaker_ops.tui import ProcessingJobsApp, PipelineExecutionsApp, SmopsTuiApp


console = Console()
app = typer.Typer(help="SageMaker Processing Job and Pipeline operations CLI.", no_args_is_help=True)
processing_app = typer.Typer(help="Submit and inspect SageMaker Processing Jobs.", no_args_is_help=True)
pipeline_app = typer.Typer(help="Start and inspect SageMaker Pipelines.", no_args_is_help=True)
ecs_app = typer.Typer(help="Inspect Amazon ECS clusters, services, tasks, and logs.", no_args_is_help=True)
tui_app = typer.Typer(help="Interactive TUI views.", invoke_without_command=True, no_args_is_help=False)
config_app = typer.Typer(help="Manage smops defaults.", no_args_is_help=True)
app.add_typer(processing_app, name="processing")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(ecs_app, name="ecs")
app.add_typer(tui_app, name="tui")
app.add_typer(config_app, name="config")


def version_callback(value: bool) -> None:
    if value:
        console.print(f"smops {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=version_callback, help="Show version."),
    ] = False,
) -> None:
    _ = version


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _print_json(payload: dict[str, Any]) -> None:
    console.print_json(json.dumps(_jsonable(payload), default=str))


def _exit_error(exc: AwsCliError, json_output: bool) -> None:
    if json_output:
        _print_json({"status": "error", "error": str(exc)})
    else:
        console.print(f"[red]{exc}[/red]")
    raise typer.Exit(1) from exc


def _effective_region(region: str | None) -> str | None:
    try:
        return resolve_region(region)
    except ValueError as exc:
        raise AwsCliError(str(exc)) from exc


@config_app.command("set-region")
def config_set_region(
    region: Annotated[str, typer.Argument(help="Default AWS region for smops commands.")],
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Set the default AWS region used by smops."""
    try:
        set_default_region(region)
    except ValueError as exc:
        _exit_error(AwsCliError(str(exc)), json_output)
    payload = {"status": "ok", "default_region": region, "config_path": str(config_path())}
    if json_output:
        _print_json(payload)
        return
    console.print(f"Default region set to [bold]{region}[/bold]")
    console.print(f"Config: {config_path()}")


@config_app.command("get-region")
def config_get_region(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Print the configured default AWS region."""
    try:
        region = get_default_region()
    except ValueError as exc:
        _exit_error(AwsCliError(str(exc)), json_output)
    payload = {"status": "ok", "default_region": region, "config_path": str(config_path())}
    if json_output:
        _print_json(payload)
        return
    if region:
        console.print(region)
    else:
        console.print("No default region configured.")


@config_app.command("show")
def config_show(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Show smops configuration."""
    try:
        config = load_config()
        region = get_default_region()
    except ValueError as exc:
        _exit_error(AwsCliError(str(exc)), json_output)
    payload = {
        "status": "ok",
        "config_path": str(config_path()),
        "default_region": region,
        "config": config,
    }
    if json_output:
        _print_json(payload)
        return
    console.print_json(json.dumps(_jsonable(payload), default=str))


@config_app.command("path")
def config_path_command() -> None:
    """Print the smops config file path."""
    console.print(str(config_path()))


@processing_app.command("submit")
def processing_submit(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, help="JSON/YAML file using create_processing_job parameters.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print the request without submitting it.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Submit a SageMaker Processing Job."""
    try:
        spec = load_job_spec(config)
        if dry_run:
            payload = {"status": "ok", "dry_run": True, "request": spec}
            if json_output:
                _print_json(payload)
            else:
                console.print_json(json.dumps(_jsonable(spec), default=str))
            return
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        response = submit_processing_job(ctx, spec)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json(
            {
                "status": "ok",
                "profile": ctx.profile,
                "region": ctx.region,
                "processing_job_name": spec.get("ProcessingJobName"),
                "response": response,
            }
        )
        return
    console.print("[green]Processing job 已提交[/green]")
    console.print_json(json.dumps(response, default=str))


@processing_app.command("list")
def processing_list(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    max_results: Annotated[int, typer.Option("--max-results", min=1, max=100, help="Max running jobs to read per page.")] = 20,
    next_token: Annotated[str | None, typer.Option("--next-token", help="Next token printed by the previous page.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List running Processing Jobs one page at a time."""
    try:
        if json_output:
            contexts = build_contexts(tuple(profile or ()), _effective_region(region), all_profiles=all_profiles)
            if next_token and len(contexts) != 1:
                raise AwsCliError("--next-token 只支持单个 AWS profile 查询")
            pages = [list_processing_jobs_page(ctx, page_size=max_results, next_token=next_token) for ctx in contexts]
        else:
            with console.status("正在查询 SageMaker Processing Jobs..."):
                contexts = build_contexts(tuple(profile or ()), _effective_region(region), all_profiles=all_profiles)
                if next_token and len(contexts) != 1:
                    raise AwsCliError("--next-token 只支持单个 AWS profile 查询")
                pages = [list_processing_jobs_page(ctx, page_size=max_results, next_token=next_token) for ctx in contexts]
        jobs = [job for page in pages for job in page.jobs]
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    next_tokens = [page.next_token for page in pages if page.next_token]
    if json_output:
        _print_json(
            {
                "status": "ok",
                "items": jobs,
                "count": len(jobs),
                "next_token": next_tokens[0] if len(contexts) == 1 and next_tokens else None,
            }
        )
        return

    if not jobs:
        console.print("[yellow]当前查询范围没有正在运行的 processing jobs。[/yellow]")
        if len(contexts) == 1 and next_tokens:
            console.print(f"Next token: {next_tokens[0]}")
        return

    table = Table("Profile", "Region", "Job", "Status", "Runtime", "Instance", "Created")
    for job in jobs:
        instance = job.instance_type
        if job.instance_count:
            instance = f"{job.instance_count}x {instance}"
        table.add_row(
            job.profile,
            job.region,
            job.name,
            job.status,
            format_duration(job.started_time or job.creation_time),
            instance,
            format_dt(job.creation_time),
        )
    console.print(table)
    if len(contexts) == 1 and next_tokens:
        console.print(f"Next token: {next_tokens[0]}")


@processing_app.command("wait")
def processing_wait(
    name: Annotated[str, typer.Option("--name", "-n", help="Processing Job name.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    timeout: Annotated[int, typer.Option("--timeout", min=0, help="Max seconds to wait.")] = 3600,
    poll: Annotated[int, typer.Option("--poll", min=1, help="Polling interval in seconds.")] = 30,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Wait until a Processing Job reaches a terminal state."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        job = wait_processing_job(ctx, name, timeout_seconds=timeout, poll_seconds=poll)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    payload = {"status": "ok", "profile": ctx.profile, "region": ctx.region, "processing_job": job}
    if json_output:
        _print_json(payload)
        return
    console.print(f"[bold]{job.name}[/bold] {job.status}")
    if job.failure_reason:
        console.print(job.failure_reason)


@pipeline_app.command("start")
def pipeline_start(
    name: Annotated[str, typer.Option("--name", "-n", help="SageMaker Pipeline name.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    display_name: Annotated[str | None, typer.Option("--display-name", help="Pipeline execution display name.")] = None,
    parameter: Annotated[list[str] | None, typer.Option("--parameter", help="Pipeline parameter in NAME=VALUE form. Can be repeated.")] = None,
    client_request_token: Annotated[str | None, typer.Option("--client-request-token", help="Idempotency token.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Start a SageMaker Pipeline execution."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        response = start_pipeline_execution(
            ctx,
            pipeline_name=name,
            display_name=display_name,
            parameters=parse_parameters(parameter or ()),
            client_request_token=client_request_token,
        )
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json(
            {
                "status": "ok",
                "profile": ctx.profile,
                "region": ctx.region,
                "pipeline_name": name,
                "pipeline_execution_arn": response.get("PipelineExecutionArn"),
                "response": response,
            }
        )
        return
    console.print("[green]Pipeline execution 已启动[/green]")
    console.print_json(json.dumps(response, default=str))


@pipeline_app.command("list")
def pipeline_list(
    pipeline_name: Annotated[str | None, typer.Option("--name", "-n", help="Only inspect one Pipeline.")] = None,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    per_pipeline: Annotated[int, typer.Option("--per-pipeline", min=1, max=100, help="Max executions to read per pipeline.")] = 10,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="Include executions completed within this many hours.")] = 3,
    pipeline_page_size: Annotated[int, typer.Option("--pipeline-page-size", min=1, max=100, help="How many pipelines to scan per page when --name is omitted.")] = 10,
    next_token: Annotated[str | None, typer.Option("--next-token", help="Next token printed by the previous page.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List running and recently completed Pipeline executions."""
    try:
        if json_output:
            contexts = build_contexts(tuple(profile or ()), _effective_region(region), all_profiles=all_profiles)
            if next_token and (len(contexts) != 1 or pipeline_name):
                raise AwsCliError("--next-token 只支持单个 AWS profile 且不指定 --name 的查询")
            pages = [
                list_pipeline_executions_page(
                    ctx,
                    pipeline_name=pipeline_name,
                    per_pipeline=per_pipeline,
                    recent_hours=hours,
                    pipeline_page_size=pipeline_page_size,
                    next_token=next_token,
                )
                for ctx in contexts
            ]
        else:
            with console.status("正在查询 SageMaker Pipeline executions..."):
                contexts = build_contexts(tuple(profile or ()), _effective_region(region), all_profiles=all_profiles)
                if next_token and (len(contexts) != 1 or pipeline_name):
                    raise AwsCliError("--next-token 只支持单个 AWS profile 且不指定 --name 的查询")
                pages = [
                    list_pipeline_executions_page(
                        ctx,
                        pipeline_name=pipeline_name,
                        per_pipeline=per_pipeline,
                        recent_hours=hours,
                        pipeline_page_size=pipeline_page_size,
                        next_token=next_token,
                    )
                    for ctx in contexts
                ]
        executions = [item for page in pages for item in page.executions]
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    next_tokens = [page.next_token for page in pages if page.next_token]
    if json_output:
        _print_json(
            {
                "status": "ok",
                "items": executions,
                "count": len(executions),
                "recent_hours": hours,
                "next_token": next_tokens[0] if len(contexts) == 1 and next_tokens else None,
            }
        )
        return

    if not executions:
        target = f"Pipeline {pipeline_name}" if pipeline_name else "当前页"
        console.print(f"[yellow]{target} 没有正在运行或最近 {hours} 小时内结束的 pipeline executions。[/yellow]")
        if len(contexts) == 1 and next_tokens:
            console.print(f"Next token: {next_tokens[0]}")
        return

    table = Table("Profile", "Region", "Pipeline", "Execution", "Status", "Runtime", "Started")
    for execution in executions:
        table.add_row(
            execution.profile,
            execution.region,
            execution.pipeline_name,
            execution.display_name or execution.execution_arn.rsplit("/", 1)[-1],
            execution.status,
            format_duration(execution.start_time),
            format_dt(execution.start_time),
        )
    console.print(table)
    if len(contexts) == 1 and next_tokens:
        console.print(f"Next token: {next_tokens[0]}")


@pipeline_app.command("steps")
def pipeline_steps(
    execution_arn: Annotated[str, typer.Option("--execution-arn", "-e", help="Pipeline execution ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Show steps for one Pipeline execution."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        detail = describe_pipeline_execution(ctx, execution_arn)
        steps = list_pipeline_steps(ctx, execution_arn)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json(
            {
                "status": "ok",
                "profile": ctx.profile,
                "region": ctx.region,
                "execution": detail,
                "items": steps,
                "count": len(steps),
            }
        )
        return

    console.print(f"[bold]{detail.get('PipelineName', '')}[/bold] {detail.get('PipelineExecutionStatus', '')}")
    table = Table("Step", "Type", "Status", "Runtime", "Failure")
    for step in steps:
        table.add_row(
            step.get("StepName", ""),
            step.get("StepType", ""),
            step.get("StepStatus", ""),
            format_duration(step.get("StartTime"), step.get("EndTime")),
            step.get("FailureReason", ""),
        )
    console.print(table)


@pipeline_app.command("wait")
def pipeline_wait(
    execution_arn: Annotated[str, typer.Option("--execution-arn", "-e", help="Pipeline execution ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    timeout: Annotated[int, typer.Option("--timeout", min=0, help="Max seconds to wait.")] = 3600,
    poll: Annotated[int, typer.Option("--poll", min=1, help="Polling interval in seconds.")] = 30,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Wait until a Pipeline execution reaches a terminal state."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        detail = wait_pipeline_execution(ctx, execution_arn, timeout_seconds=timeout, poll_seconds=poll)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "execution": detail})
        return
    console.print(f"[bold]{detail.get('PipelineName', '')}[/bold] {detail.get('PipelineExecutionStatus', '')}")
    if detail.get("FailureReason"):
        console.print(detail["FailureReason"])


@pipeline_app.command("inspect")
def pipeline_inspect(
    execution_arn: Annotated[str, typer.Option("--execution-arn", "-e", help="Pipeline execution ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Inspect one Pipeline execution, including steps and failed steps."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        inspection = inspect_pipeline_execution(ctx, execution_arn)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", **inspection})
        return

    detail = inspection["execution"]
    console.print(f"[bold]{detail.get('PipelineName', '')}[/bold] {detail.get('PipelineExecutionStatus', '')}")
    table = Table("Step", "Type", "Status", "Runtime", "Failure")
    for step in inspection["steps"]:
        table.add_row(
            step.get("StepName", ""),
            step.get("StepType", ""),
            step.get("StepStatus", ""),
            format_duration(step.get("StartTime"), step.get("EndTime")),
            step.get("FailureReason", ""),
        )
    console.print(table)


@pipeline_app.command("diagnose")
def pipeline_diagnose(
    execution_arn: Annotated[str, typer.Option("--execution-arn", "-e", help="Pipeline execution ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    log_limit: Annotated[int, typer.Option("--log-limit", min=1, max=500, help="Max CloudWatch log lines to return.")] = 80,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Diagnose a Pipeline execution and load logs for the first failed step."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        diagnosis = diagnose_pipeline_execution(ctx, execution_arn, log_limit=log_limit)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", **diagnosis})
        return

    detail = diagnosis["execution"]
    failed_step = diagnosis["failed_step"]
    console.print(f"[bold]{detail.get('PipelineName', '')}[/bold] {detail.get('PipelineExecutionStatus', '')}")
    if not failed_step:
        console.print("[green]No failed step found.[/green]")
        return
    console.print(f"[red]Failed step:[/red] {failed_step.get('StepName', '')}")
    if failed_step.get("FailureReason"):
        console.print(failed_step["FailureReason"])
    if diagnosis.get("log_group"):
        console.print(f"Logs: {diagnosis['log_group']} / {diagnosis['log_stream_prefix']}")
    for line in diagnosis["log_tail"]:
        console.print(line)


@ecs_app.command("clusters")
def ecs_clusters(
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List ECS clusters."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        clusters = list_ecs_clusters(ctx)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "items": clusters, "count": len(clusters)})
        return

    if not clusters:
        console.print("[yellow]No ECS clusters found.[/yellow]")
        return
    table = Table("Profile", "Region", "Cluster", "Status", "Running", "Pending", "Services")
    for cluster in clusters:
        table.add_row(
            cluster.profile,
            cluster.region,
            cluster.cluster_name or cluster.cluster_arn,
            cluster.status,
            str(cluster.running_tasks),
            str(cluster.pending_tasks),
            str(cluster.active_services),
        )
    console.print(table)


@ecs_app.command("services")
def ecs_services(
    cluster: Annotated[str, typer.Option("--cluster", "-c", help="ECS cluster name or ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List ECS services in a cluster."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        services = list_ecs_services(ctx, cluster)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "cluster": cluster, "items": services, "count": len(services)})
        return

    if not services:
        console.print(f"[yellow]No ECS services found in cluster {cluster}.[/yellow]")
        return
    table = Table("Profile", "Region", "Service", "Status", "Desired", "Running", "Pending", "Task Definition")
    for service in services:
        table.add_row(
            service.profile,
            service.region,
            service.service_name or service.service_arn,
            service.status,
            str(service.desired_count),
            str(service.running_count),
            str(service.pending_count),
            service.task_definition.rsplit("/", 1)[-1],
        )
    console.print(table)


@ecs_app.command("tasks")
def ecs_tasks(
    cluster: Annotated[str, typer.Option("--cluster", "-c", help="ECS cluster name or ARN.")],
    service: Annotated[str | None, typer.Option("--service", "-s", help="Filter tasks by ECS service name.")] = None,
    status: Annotated[str, typer.Option("--status", help="Desired task status: RUNNING, STOPPED, or PENDING.")] = "RUNNING",
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List ECS tasks in a cluster."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        tasks = list_ecs_tasks(ctx, cluster, service=service, desired_status=status)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "cluster": cluster, "items": tasks, "count": len(tasks)})
        return

    if not tasks:
        console.print(f"[yellow]No ECS {status.upper()} tasks found in cluster {cluster}.[/yellow]")
        return
    table = Table("Profile", "Region", "Task", "Last", "Desired", "Launch", "Started", "Containers")
    for task in tasks:
        table.add_row(
            task.profile,
            task.region,
            task.task_id,
            task.last_status,
            task.desired_status,
            task.launch_type,
            format_dt(task.started_at),
            ", ".join(container.get("name", "") for container in task.containers),
        )
    console.print(table)


@ecs_app.command("task")
def ecs_task(
    cluster: Annotated[str, typer.Option("--cluster", "-c", help="ECS cluster name or ARN.")],
    task: Annotated[str, typer.Option("--task", "-t", help="ECS task ID or ARN.")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Describe one ECS task."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        task_view = describe_ecs_task(ctx, cluster, task)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "cluster": cluster, "task": task_view})
        return

    console.print(f"[bold]{task_view.task_id}[/bold] {task_view.last_status} desired={task_view.desired_status}")
    console.print(f"Cluster: {task_view.cluster}")
    console.print(f"Task definition: {task_view.task_definition_arn}")
    if task_view.stopped_reason:
        console.print(f"Stopped reason: {task_view.stopped_reason}")
    table = Table("Container", "Last Status", "Exit", "Reason", "Runtime ID")
    for container in task_view.containers:
        table.add_row(
            container.get("name", ""),
            container.get("last_status", ""),
            "" if container.get("exit_code") is None else str(container.get("exit_code")),
            container.get("reason", ""),
            container.get("runtime_id", ""),
        )
    console.print(table)


@ecs_app.command("logs")
def ecs_logs(
    cluster: Annotated[str, typer.Option("--cluster", "-c", help="ECS cluster name or ARN.")],
    task: Annotated[str, typer.Option("--task", "-t", help="ECS task ID or ARN.")],
    container: Annotated[str | None, typer.Option("--container", help="Container name when the task has multiple awslogs containers.")] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=500, help="Max CloudWatch log lines to return.")] = 80,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Tail CloudWatch logs for one ECS task container using awslogs config."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        lines = tail_ecs_task_logs(ctx, cluster, task, container=container, limit=limit)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "cluster": cluster, "task": task, "lines": lines, "count": len(lines)})
        return

    for line in lines:
        console.print(line)


@tui_app.callback(invoke_without_command=True)
def tui_main(
    ctx: typer.Context,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="Include pipeline executions completed within this many hours.")] = 3,
) -> None:
    """Open the smops TUI selector when no TUI subcommand is provided."""
    if ctx.invoked_subcommand is not None:
        return
    selected = SmopsTuiApp().run()
    if selected == "processing":
        ProcessingJobsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()
    elif selected == "pipelines":
        PipelineExecutionsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh, None, hours).run()


@tui_app.command("processing")
def tui_processing(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
) -> None:
    """Interactively inspect running Processing Jobs."""
    ProcessingJobsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()


@tui_app.command("pipelines")
def tui_pipelines(
    pipeline_name: Annotated[str | None, typer.Option("--name", "-n", help="Only inspect one Pipeline.")] = None,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="Include executions completed within this many hours.")] = 3,
) -> None:
    """Interactively inspect running and recently completed Pipeline executions, steps, and failed logs."""
    PipelineExecutionsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh, pipeline_name, hours).run()


if __name__ == "__main__":
    app()
