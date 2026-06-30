from __future__ import annotations

from typing import Any

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, RichLog, Static

from sagemaker_ops.aws import (
    AwsCliError,
    AwsContext,
    ProcessingJobView,
    PipelineExecutionView,
    build_contexts,
    format_dt,
    format_duration,
    infer_log_source,
    list_active_pipeline_executions,
    list_pipeline_steps,
    list_processing_jobs,
    tail_step_logs,
)


class BaseSageMakerApp(App[None]):
    CSS = """
    Screen {
        layout: vertical;
    }

    #content {
        height: 1fr;
    }

    DataTable {
        height: 1fr;
        border: solid $surface;
    }

    #detail, #logs {
        height: 1fr;
        border: solid $surface;
        padding: 1;
    }

    #detail {
        width: 40%;
    }

    #left-pane, #right-pane {
        height: 1fr;
    }

    #left-pane {
        width: 52%;
    }

    #right-pane {
        width: 48%;
    }
    """

    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("q", "quit", "Quit"),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
    ) -> None:
        super().__init__()
        self.profiles = profiles
        self.region = region
        self.all_profiles = all_profiles
        self.refresh_seconds = refresh_seconds
        self.contexts: list[AwsContext] = []

    def load_contexts(self) -> list[AwsContext]:
        if not self.contexts:
            self.contexts = build_contexts(self.profiles, self.region, self.all_profiles)
        return self.contexts

    def on_mount(self) -> None:
        self.set_interval(self.refresh_seconds, self.action_refresh)
        self.action_refresh()

    def show_error(self, exc: Exception) -> None:
        self.query_one("#status", Static).update(f"[red]{exc}[/red]")


class ProcessingJobsApp(BaseSageMakerApp):
    TITLE = "SageMaker Processing Jobs"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        ("left", "previous_job", "Previous"),
        ("right", "next_job", "Next"),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
    ) -> None:
        super().__init__(profiles, region, all_profiles, refresh_seconds)
        self.jobs: list[ProcessingJobView] = []

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Horizontal(id="content"):
            table = DataTable(id="jobs")
            table.cursor_type = "row"
            yield table
            yield Static("", id="detail")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#jobs", DataTable)
        table.add_columns("Profile", "Region", "Job", "Status", "Runtime", "Instance", "Created")
        super().on_mount()

    def action_refresh(self) -> None:
        try:
            self.jobs = [job for ctx in self.load_contexts() for job in list_processing_jobs(ctx)]
            self.render_jobs()
            self.query_one("#status", Static).update(
                f"{len(self.jobs)} running processing job(s). Refresh every {self.refresh_seconds}s. "
                "Use arrows to move, r to refresh, q to quit."
            )
        except AwsCliError as exc:
            self.show_error(exc)

    def action_previous_job(self) -> None:
        table = self.query_one("#jobs", DataTable)
        if table.row_count:
            table.move_cursor(row=max(0, table.cursor_row - 1))

    def action_next_job(self) -> None:
        table = self.query_one("#jobs", DataTable)
        if table.row_count:
            table.move_cursor(row=min(table.row_count - 1, table.cursor_row + 1))

    def render_jobs(self) -> None:
        table = self.query_one("#jobs", DataTable)
        table.clear()
        for index, job in enumerate(self.jobs):
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
                key=str(index),
            )
        self.update_processing_detail(0 if self.jobs else None)

    @on(DataTable.RowHighlighted, "#jobs")
    def on_job_highlighted(self, event: DataTable.RowHighlighted) -> None:
        try:
            self.update_processing_detail(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    def update_processing_detail(self, index: int | None) -> None:
        detail = self.query_one("#detail", Static)
        if index is None or index >= len(self.jobs):
            detail.update("No running processing jobs.")
            return
        job = self.jobs[index]
        detail.update(
            "\n".join(
                [
                    f"[bold]{job.name}[/bold]",
                    f"Profile: {job.profile}",
                    f"Region: {job.region}",
                    f"Status: {job.status}",
                    f"Created: {format_dt(job.creation_time)}",
                    f"Started: {format_dt(job.started_time)}",
                    f"Runtime: {format_duration(job.started_time or job.creation_time)}",
                    f"Instance: {job.instance_count or ''}x {job.instance_type}".strip(),
                    f"Role: {job.role_arn}",
                    f"Arn: {job.arn}",
                    f"Failure: {job.failure_reason}" if job.failure_reason else "",
                ]
            )
        )


class PipelineExecutionsApp(BaseSageMakerApp):
    TITLE = "SageMaker Pipeline Executions"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        ("left", "focus_executions", "Executions"),
        ("right", "focus_steps", "Steps"),
        ("l", "load_logs", "Logs"),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
        pipeline_name: str | None,
    ) -> None:
        super().__init__(profiles, region, all_profiles, refresh_seconds)
        self.pipeline_name = pipeline_name
        self.executions: list[tuple[AwsContext, PipelineExecutionView]] = []
        self.steps: list[dict[str, Any]] = []
        self.selected_context: AwsContext | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Horizontal(id="content"):
            with Vertical(id="left-pane"):
                executions = DataTable(id="executions")
                executions.cursor_type = "row"
                yield executions
            with Vertical(id="right-pane"):
                steps = DataTable(id="steps")
                steps.cursor_type = "row"
                yield steps
                yield RichLog(id="logs", wrap=True, highlight=True)
        yield Footer()

    def on_mount(self) -> None:
        executions = self.query_one("#executions", DataTable)
        executions.add_columns("Profile", "Region", "Pipeline", "Execution", "Status", "Runtime")
        steps = self.query_one("#steps", DataTable)
        steps.add_columns("Step", "Type", "Status", "Runtime", "Failure")
        super().on_mount()

    def action_refresh(self) -> None:
        try:
            pairs: list[tuple[AwsContext, PipelineExecutionView]] = []
            for ctx in self.load_contexts():
                for execution in list_active_pipeline_executions(ctx, pipeline_name=self.pipeline_name):
                    pairs.append((ctx, execution))
            self.executions = pairs
            self.render_executions()
            self.query_one("#status", Static).update(
                f"{len(self.executions)} running pipeline execution(s). "
                "Use left/right to switch panes, arrows to move, l to load failed-step logs."
            )
        except AwsCliError as exc:
            self.show_error(exc)

    def action_focus_executions(self) -> None:
        self.query_one("#executions", DataTable).focus()

    def action_focus_steps(self) -> None:
        self.query_one("#steps", DataTable).focus()

    def action_load_logs(self) -> None:
        self.load_selected_step_logs()

    def render_executions(self) -> None:
        table = self.query_one("#executions", DataTable)
        table.clear()
        for index, (_, execution) in enumerate(self.executions):
            table.add_row(
                execution.profile,
                execution.region,
                execution.pipeline_name,
                execution.display_name or execution.execution_arn.rsplit("/", 1)[-1],
                execution.status,
                format_duration(execution.start_time),
                key=str(index),
            )
        self.update_steps(0 if self.executions else None)

    @on(DataTable.RowHighlighted, "#executions")
    def on_execution_highlighted(self, event: DataTable.RowHighlighted) -> None:
        try:
            self.update_steps(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    @on(DataTable.RowHighlighted, "#steps")
    def on_step_highlighted(self, _: DataTable.RowHighlighted) -> None:
        self.render_step_failure_or_hint()

    def update_steps(self, index: int | None) -> None:
        self.steps = []
        self.selected_context = None
        logs = self.query_one("#logs", RichLog)
        logs.clear()
        if index is None or index >= len(self.executions):
            self.query_one("#steps", DataTable).clear()
            logs.write("No running pipeline executions.")
            return

        ctx, execution = self.executions[index]
        self.selected_context = ctx
        try:
            self.steps = list_pipeline_steps(ctx, execution.execution_arn)
        except AwsCliError as exc:
            logs.write(str(exc))
            return
        table = self.query_one("#steps", DataTable)
        table.clear()
        for step_index, step in enumerate(self.steps):
            table.add_row(
                step.get("StepName", ""),
                step.get("StepType", ""),
                step.get("StepStatus", ""),
                format_duration(step.get("StartTime"), step.get("EndTime")),
                _shorten(step.get("FailureReason", ""), 80),
                key=str(step_index),
            )
        self.render_step_failure_or_hint()

    def render_step_failure_or_hint(self) -> None:
        logs = self.query_one("#logs", RichLog)
        logs.clear()
        step = self.selected_step()
        if step is None:
            logs.write("Select a step to view details.")
            return
        failure = step.get("FailureReason")
        if failure:
            logs.write(f"[bold red]Failure[/bold red] {failure}")
            source = infer_log_source(step)
            if source:
                logs.write("Press l to load CloudWatch log tail.")
            else:
                logs.write("This step type has no supported CloudWatch log source yet.")
            return
        logs.write(
            f"{step.get('StepName', '')} {step.get('StepStatus', '')} "
            f"{format_duration(step.get('StartTime'), step.get('EndTime'))}"
        )

    def load_selected_step_logs(self) -> None:
        ctx = self.selected_context
        step = self.selected_step()
        logs = self.query_one("#logs", RichLog)
        logs.clear()
        if ctx is None or step is None:
            logs.write("Select a pipeline step first.")
            return
        failure = step.get("FailureReason")
        if failure:
            logs.write(f"[bold red]Failure[/bold red] {failure}")
        for line in tail_step_logs(ctx, step):
            logs.write(line)

    def selected_step(self) -> dict[str, Any] | None:
        table = self.query_one("#steps", DataTable)
        if not self.steps or table.cursor_row >= len(self.steps):
            return None
        return self.steps[table.cursor_row]


def _shorten(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"

