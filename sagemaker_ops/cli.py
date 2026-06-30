from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from sagemaker_ops import __version__
from sagemaker_ops.aws import (
    AwsCliError,
    build_contexts,
    describe_pipeline_execution,
    format_dt,
    format_duration,
    list_active_pipeline_executions,
    list_pipeline_executions_page,
    list_pipeline_steps,
    list_processing_jobs,
    list_processing_jobs_page,
    load_job_spec,
    parse_parameters,
    start_pipeline_execution,
    submit_processing_job,
)
from sagemaker_ops.tui import ProcessingJobsApp, PipelineExecutionsApp


console = Console()
app = typer.Typer(help="SageMaker Processing Job 与 Pipeline 运维 CLI。", no_args_is_help=True)
processing_app = typer.Typer(help="提交和查看 SageMaker Processing Job。", no_args_is_help=True)
pipeline_app = typer.Typer(help="启动和查看 SageMaker Pipeline。", no_args_is_help=True)
tui_app = typer.Typer(help="交互式 TUI。", no_args_is_help=True)
app.add_typer(processing_app, name="processing")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(tui_app, name="tui")


def version_callback(value: bool) -> None:
    if value:
        console.print(f"smops {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=version_callback, help="显示版本。"),
    ] = False,
) -> None:
    _ = version


@processing_app.command("submit")
def processing_submit(
    config: Annotated[Path, typer.Option("--config", "-c", exists=True, help="create_processing_job 的 JSON/YAML 参数文件。")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="只打印请求，不提交。")] = False,
) -> None:
    """提交 SageMaker Processing Job。"""
    try:
        spec = load_job_spec(config)
        if dry_run:
            console.print_json(json.dumps(spec, default=str))
            return
        ctx = build_contexts((profile,) if profile else (), region)[0]
        response = submit_processing_job(ctx, spec)
    except AwsCliError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    console.print("[green]Processing job 已提交[/green]")
    console.print_json(json.dumps(response, default=str))


@processing_app.command("list")
def processing_list(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile，可重复传。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="查看本机所有 AWS profiles。")] = False,
    max_results: Annotated[int, typer.Option("--max-results", min=1, max=100, help="每页最多读取多少个 running job。")] = 20,
    next_token: Annotated[str | None, typer.Option("--next-token", help="上一页输出的 Next token。")] = None,
) -> None:
    """按页列出正在运行的 Processing Jobs。"""
    try:
        with console.status("正在查询 SageMaker Processing Jobs..."):
            contexts = build_contexts(tuple(profile or ()), region, all_profiles=all_profiles)
            if next_token and len(contexts) != 1:
                raise AwsCliError("--next-token 只支持单个 AWS profile 查询")
            pages = [list_processing_jobs_page(ctx, page_size=max_results, next_token=next_token) for ctx in contexts]
            jobs = [job for page in pages for job in page.jobs]
    except AwsCliError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc

    next_tokens = [page.next_token for page in pages if page.next_token]
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


@pipeline_app.command("start")
def pipeline_start(
    name: Annotated[str, typer.Option("--name", "-n", help="SageMaker Pipeline 名称。")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    display_name: Annotated[str | None, typer.Option("--display-name", help="Pipeline execution display name。")] = None,
    parameter: Annotated[list[str] | None, typer.Option("--parameter", help="Pipeline 参数，格式 NAME=VALUE，可重复传。")] = None,
    client_request_token: Annotated[str | None, typer.Option("--client-request-token", help="幂等 token。")] = None,
) -> None:
    """启动 SageMaker Pipeline execution。"""
    try:
        ctx = build_contexts((profile,) if profile else (), region)[0]
        response = start_pipeline_execution(
            ctx,
            pipeline_name=name,
            display_name=display_name,
            parameters=parse_parameters(parameter or ()),
            client_request_token=client_request_token,
        )
    except AwsCliError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    console.print("[green]Pipeline execution 已启动[/green]")
    console.print_json(json.dumps(response, default=str))


@pipeline_app.command("list")
def pipeline_list(
    pipeline_name: Annotated[str | None, typer.Option("--name", "-n", help="只查看某个 Pipeline。")] = None,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile，可重复传。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="查看本机所有 AWS profiles。")] = False,
    per_pipeline: Annotated[int, typer.Option("--per-pipeline", min=1, max=100, help="每个 pipeline 最多读取多少个 execution。")] = 10,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="额外显示最近多少小时内结束的 executions。")] = 3,
    pipeline_page_size: Annotated[int, typer.Option("--pipeline-page-size", min=1, max=100, help="不传 --name 时每页扫描多少个 pipelines。")] = 10,
    next_token: Annotated[str | None, typer.Option("--next-token", help="上一页输出的 Next token。")] = None,
) -> None:
    """用表格列出正在运行和最近结束的 Pipeline executions。"""
    try:
        with console.status("正在查询 SageMaker Pipeline executions..."):
            contexts = build_contexts(tuple(profile or ()), region, all_profiles=all_profiles)
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
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc

    next_tokens = [page.next_token for page in pages if page.next_token]
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
    execution_arn: Annotated[str, typer.Option("--execution-arn", "-e", help="Pipeline execution ARN。")],
    profile: Annotated[str | None, typer.Option("--profile", "-p", help="AWS profile。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
) -> None:
    """查看某个 Pipeline execution 的 steps。"""
    try:
        ctx = build_contexts((profile,) if profile else (), region)[0]
        detail = describe_pipeline_execution(ctx, execution_arn)
        steps = list_pipeline_steps(ctx, execution_arn)
    except AwsCliError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc

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


@tui_app.command("processing")
def tui_processing(
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile，可重复传。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="查看本机所有 AWS profiles。")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="刷新间隔秒数。")] = 15,
) -> None:
    """交互式查看正在运行的 Processing Jobs。"""
    ProcessingJobsApp(tuple(profile or ()), region, all_profiles, refresh).run()


@tui_app.command("pipelines")
def tui_pipelines(
    pipeline_name: Annotated[str | None, typer.Option("--name", "-n", help="只查看某个 Pipeline。")] = None,
    profile: Annotated[list[str] | None, typer.Option("--profile", "-p", help="AWS profile，可重复传。")] = None,
    region: Annotated[str | None, typer.Option("--region", "-r", help="AWS region。")] = None,
    all_profiles: Annotated[bool, typer.Option("--all-profiles", help="查看本机所有 AWS profiles。")] = False,
    refresh: Annotated[int, typer.Option("--refresh", min=5, max=300, help="刷新间隔秒数。")] = 15,
    hours: Annotated[int, typer.Option("--hours", min=1, max=168, help="额外显示最近多少小时内结束的 executions。")] = 3,
) -> None:
    """交互式查看正在运行和最近结束的 Pipeline executions、steps 和失败日志。"""
    PipelineExecutionsApp(tuple(profile or ()), region, all_profiles, refresh, pipeline_name, hours).run()


if __name__ == "__main__":
    app()

