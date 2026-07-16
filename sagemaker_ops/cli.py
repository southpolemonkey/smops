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
from sagemaker_ops.config import (
    config_path,
    get_default_mwaa_environment,
    get_default_region,
    load_config,
    resolve_mwaa_environment,
    resolve_region,
    set_default_mwaa_environment,
    set_default_region,
)
from sagemaker_ops.aws import (
    AwsCliError,
    build_contexts,
    describe_ecs_task,
    EcsTaskHistoryEntry,
    list_airflow_dag_runs,
    list_airflow_dags,
    list_airflow_pools,
    list_airflow_task_instances,
    list_ecs_task_history,
    list_mwaa_environments,
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
    trigger_airflow_dag,
    wait_pipeline_execution,
    wait_processing_job,
)
from sagemaker_ops.tui import AirflowApp, EcsTasksApp, ProcessingJobsApp, PipelineExecutionsApp, SmopsTuiApp


console = Console()
app = typer.Typer(help="SageMaker Processing Job and Pipeline operations CLI.", no_args_is_help=True)
processing_app = typer.Typer(help="Submit and inspect SageMaker Processing Jobs.", no_args_is_help=True)
pipeline_app = typer.Typer(help="Start and inspect SageMaker Pipelines.", no_args_is_help=True)
ecs_app = typer.Typer(help="Inspect Amazon ECS clusters, services, tasks, and logs.", no_args_is_help=True)
airflow_app = typer.Typer(help="Monitor and trigger Amazon MWAA (Apache Airflow) DAGs.", no_args_is_help=True)
tui_app = typer.Typer(help="Interactive TUI views.", invoke_without_command=True, no_args_is_help=False)
config_app = typer.Typer(help="Manage smops defaults.", no_args_is_help=True)
app.add_typer(processing_app, name="processing")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(ecs_app, name="ecs")
app.add_typer(airflow_app, name="airflow")
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


def _effective_mwaa_environment(environment: str | None) -> str:
    try:
        resolved = resolve_mwaa_environment(environment)
    except ValueError as exc:
        raise AwsCliError(str(exc)) from exc
    if not resolved:
        raise AwsCliError(
            "No MWAA environment specified. Pass --env or run 'smops config set-mwaa-env <name>'."
        )
    return resolved


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


@config_app.command("set-mwaa-env")
def config_set_mwaa_env(
    environment: Annotated[str, typer.Argument(help="Default MWAA environment name for airflow commands.")],
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Set the default MWAA environment used by smops airflow commands."""
    try:
        set_default_mwaa_environment(environment)
    except ValueError as exc:
        _exit_error(AwsCliError(str(exc)), json_output)
    payload = {"status": "ok", "mwaa_environment": environment, "config_path": str(config_path())}
    if json_output:
        _print_json(payload)
        return
    console.print(f"Default MWAA environment set to [bold]{environment}[/bold]")
    console.print(f"Config: {config_path()}")


@config_app.command("get-mwaa-env")
def config_get_mwaa_env(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Print the configured default MWAA environment."""
    try:
        environment = get_default_mwaa_environment()
    except ValueError as exc:
        _exit_error(AwsCliError(str(exc)), json_output)
    payload = {"status": "ok", "mwaa_environment": environment, "config_path": str(config_path())}
    if json_output:
        _print_json(payload)
        return
    if environment:
        console.print(environment)
    else:
        console.print("No default MWAA environment configured.")


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


@ecs_app.command("history")
def ecs_history(
    log_group: Annotated[str, typer.Option("--log-group", "-g", help="CloudWatch log group name.")],
    stream_prefix: Annotated[str | None, typer.Option("--stream-prefix", "-s", help="Log stream name prefix to filter (e.g. the awslogs-stream-prefix).")] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=200, help="Max number of historical runs to return.")] = 20,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List historical ECS task runs via CloudWatch log streams.

    The ECS API only retains stopped tasks for ~1 hour. This command looks up
    CloudWatch log streams for the given log group instead, giving you a full
    run history going back as far as the log group retention allows.
    """
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        entries = list_ecs_task_history(ctx, log_group, stream_prefix or "", limit=limit)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({
            "status": "ok",
            "profile": ctx.profile,
            "region": ctx.region,
            "log_group": log_group,
            "items": [
                {
                    "task_id": e.task_id,
                    "log_stream": e.log_stream,
                    "started_at": e.started_at.isoformat(),
                    "ended_at": e.ended_at.isoformat(),
                    "duration_seconds": e.duration_seconds,
                }
                for e in entries
            ],
            "count": len(entries),
        })
        return

    if not entries:
        console.print(f"[yellow]No log streams found in {log_group}.[/yellow]")
        return

    def _fmt_dur(secs: float) -> str:
        h, rem = divmod(int(secs), 3600)
        m, s = divmod(rem, 60)
        return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"

    table = Table("#", "Started (UTC)", "Ended (UTC)", "Duration", "Task ID")
    for i, entry in enumerate(entries, 1):
        table.add_row(
            str(i),
            entry.started_at.strftime("%Y-%m-%d %H:%M"),
            entry.ended_at.strftime("%Y-%m-%d %H:%M"),
            _fmt_dur(entry.duration_seconds),
            entry.task_id,
        )
    console.print(table)


@airflow_app.command("environments")
def airflow_environments(
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List Amazon MWAA environments."""
    try:
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        environments = list_mwaa_environments(ctx)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "items": environments, "count": len(environments)})
        return

    if not environments:
        console.print("[yellow]No MWAA environments found.[/yellow]")
        return
    table = Table("Profile", "Region", "Environment", "Status", "Airflow", "Schedulers")
    for environment in environments:
        table.add_row(
            environment.profile,
            environment.region,
            environment.name,
            environment.status,
            environment.airflow_version,
            "" if environment.schedulers is None else str(environment.schedulers),
        )
    console.print(table)


@airflow_app.command("dags")
def airflow_dags(
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    pattern: Annotated[str | None, typer.Option("--pattern", help="Filter DAGs whose dag_id contains this text.")] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=100, help="Max DAGs to return.")] = 100,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List Airflow DAGs in an MWAA environment."""
    try:
        env = _effective_mwaa_environment(environment)
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        dags = list_airflow_dags(ctx, env, name_pattern=pattern, limit=limit)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "environment": env, "items": dags, "count": len(dags)})
        return

    if not dags:
        console.print(f"[yellow]No DAGs found in {env}.[/yellow]")
        return
    table = Table("DAG", "Paused", "Active", "Schedule", "Owners", "Tags")
    for dag in dags:
        table.add_row(
            dag.dag_id,
            "yes" if dag.is_paused else "no",
            "yes" if dag.is_active else "no",
            dag.schedule_interval,
            ", ".join(dag.owners),
            ", ".join(dag.tags),
        )
    console.print(table)


@airflow_app.command("runs")
def airflow_runs(
    dag: Annotated[str, typer.Option("--dag", "-d", help="DAG id.")],
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=100, help="Max recent runs to return.")] = 10,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List recent runs for one Airflow DAG."""
    try:
        env = _effective_mwaa_environment(environment)
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        runs = list_airflow_dag_runs(ctx, env, dag, limit=limit)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "environment": env, "dag_id": dag, "items": runs, "count": len(runs)})
        return

    if not runs:
        console.print(f"[yellow]No runs found for DAG {dag} in {env}.[/yellow]")
        return
    table = Table("Run", "State", "Type", "Runtime", "Started", "Trigger")
    for run in runs:
        table.add_row(
            run.dag_run_id,
            run.state,
            run.run_type,
            format_duration(run.start_date, run.end_date),
            format_dt(run.start_date),
            "external" if run.external_trigger else "scheduled",
        )
    console.print(table)


@airflow_app.command("tasks")
def airflow_tasks(
    dag: Annotated[str, typer.Option("--dag", "-d", help="DAG id.")],
    run: Annotated[str, typer.Option("--run", help="DAG run id.")],
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List task-instance states for one Airflow DAG run."""
    try:
        env = _effective_mwaa_environment(environment)
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        instances = list_airflow_task_instances(ctx, env, dag, run)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "environment": env, "dag_id": dag, "dag_run_id": run, "items": instances, "count": len(instances)})
        return

    if not instances:
        console.print(f"[yellow]No task instances found for run {run}.[/yellow]")
        return
    table = Table("Task", "State", "Try", "Runtime", "Started")
    for instance in instances:
        table.add_row(
            instance.task_id,
            instance.state,
            "" if instance.try_number is None else str(instance.try_number),
            format_duration(instance.start_date, instance.end_date),
            format_dt(instance.start_date),
        )
    console.print(table)


@airflow_app.command("pools")
def airflow_pools(
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """List Airflow pool slot usage."""
    try:
        env = _effective_mwaa_environment(environment)
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        pools = list_airflow_pools(ctx, env)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "environment": env, "items": pools, "count": len(pools)})
        return

    if not pools:
        console.print(f"[yellow]No pools found in {env}.[/yellow]")
        return
    table = Table("Pool", "Slots", "Occupied", "Running", "Queued", "Open")
    for pool in pools:
        table.add_row(
            pool.name,
            str(pool.slots),
            str(pool.occupied_slots),
            str(pool.running_slots),
            str(pool.queued_slots),
            str(pool.open_slots),
        )
    console.print(table)


@airflow_app.command("trigger")
def airflow_trigger(
    dag: Annotated[str, typer.Option("--dag", "-d", help="DAG id.")],
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    conf: Annotated[str | None, typer.Option("--conf", help="Run configuration as a JSON object.")] = None,
    logical_date: Annotated[str | None, typer.Option("--logical-date", help="Logical date in ISO 8601. Defaults to now.")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip the confirmation prompt.")] = False,
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable JSON only.")] = False,
) -> None:
    """Trigger a new run of an Airflow DAG."""
    try:
        env = _effective_mwaa_environment(environment)
        conf_payload = _parse_trigger_conf(conf)
        if not yes:
            if json_output:
                raise AwsCliError("--json trigger requires --yes to skip the confirmation prompt.")
            if not typer.confirm(f"Trigger DAG '{dag}' in environment '{env}'?"):
                console.print("Aborted.")
                raise typer.Exit(0)
        ctx = build_contexts((profile,) if profile else (), _effective_region(region))[0]
        run = trigger_airflow_dag(ctx, env, dag, conf=conf_payload, logical_date=logical_date)
    except AwsCliError as exc:
        _exit_error(exc, json_output)

    if json_output:
        _print_json({"status": "ok", "profile": ctx.profile, "region": ctx.region, "environment": env, "dag_id": dag, "dag_run": run})
        return
    console.print(f"[green]Triggered DAG[/green] {dag}")
    console.print(f"Run: {run.dag_run_id} ({run.state})")


def _parse_trigger_conf(conf: str | None) -> dict[str, Any] | None:
    if conf is None:
        return None
    try:
        parsed = json.loads(conf)
    except json.JSONDecodeError as exc:
        raise AwsCliError(f"--conf must be a valid JSON object: {exc}") from exc
    if not isinstance(parsed, dict):
        raise AwsCliError("--conf must be a JSON object")
    return parsed


@tui_app.callback(invoke_without_command=True)
def tui_main(
    ctx: typer.Context,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="Include pipeline executions completed within this many hours.")] = 3,
    environment: Annotated[str | None, typer.Option("--env", help="MWAA environment name for the airflow view.")] = None,
) -> None:
    """Open the smops TUI selector when no TUI subcommand is provided."""
    if ctx.invoked_subcommand is not None:
        return
    selected = SmopsTuiApp().run()
    if selected == "processing":
        ProcessingJobsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()
    elif selected == "pipelines":
        PipelineExecutionsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh, None, hours).run()
    elif selected == "ecs":
        EcsTasksApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()
    elif selected == "airflow":
        AirflowApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh, resolve_mwaa_environment(environment)).run()


@tui_app.command("processing")
def tui_processing(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
) -> None:
    """Interactively inspect running Processing Jobs."""
    ProcessingJobsApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()


@tui_app.command("ecs")
def tui_ecs(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
) -> None:
    """Interactively inspect ECS clusters, services, running tasks, and logs."""
    EcsTasksApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh).run()


@tui_app.command("airflow")
def tui_airflow(
    environment: Annotated[str | None, typer.Option("--env", "-e", help="MWAA environment name.")] = None,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile. Can be passed multiple times.")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region.")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="Inspect all local AWS profiles.")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="Refresh interval in seconds.")] = 15,
) -> None:
    """Interactively inspect MWAA DAGs, runs, task instances, and pools."""
    AirflowApp(tuple(profile or ()), _effective_region(region), all_profiles, refresh, resolve_mwaa_environment(environment)).run()


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
