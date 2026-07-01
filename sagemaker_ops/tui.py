from __future__ import annotations

from pathlib import Path
from typing import Any

from textual import on
from textual.binding import Binding
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, RichLog, Static

from sagemaker_ops.aws import (
    AwsCliError,
    AwsContext,
    available_profiles,
    ProcessingJobView,
    PipelineExecutionView,
    build_contexts,
    format_dt,
    format_duration,
    infer_log_source,
    list_active_pipeline_executions,
    list_pipeline_steps,
    list_processing_jobs,
    load_job_spec,
    parse_parameters,
    start_pipeline_execution,
    submit_processing_job,
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

    #pipeline-content {
        height: 1fr;
    }

    #executions-pane {
        height: 42%;
    }

    #bottom-pane {
        height: 58%;
    }

    #steps-pane {
        width: 48%;
    }

    #logs-pane {
        width: 52%;
    }

    .dialog {
        width: 70;
        height: auto;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }

    .dialog Input {
        margin-bottom: 1;
    }

    #home {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("r", "refresh", "Refresh"),
        Binding("p", "switch_profile", "Profile", priority=True),
        Binding("P", "switch_profile", "Profile", priority=True, show=False),
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

    def on_key(self, event) -> None:
        if event.key in {"p", "P"}:
            event.stop()
            self.action_switch_profile()
        elif event.key == "s":
            if isinstance(self, ProcessingJobsApp):
                event.stop()
                self.action_submit_processing()
            elif isinstance(self, PipelineExecutionsApp):
                event.stop()
                self.action_start_pipeline()

    def primary_context(self) -> AwsContext | None:
        contexts = self.load_contexts()
        return contexts[0] if contexts else None

    def action_switch_profile(self) -> None:
        try:
            profiles = available_profiles()
        except AwsCliError as exc:
            self.show_error(exc)
            return
        if not profiles:
            self.show_error(AwsCliError("No AWS profiles found."))
            return
        current = self.profiles[0] if self.profiles else None
        try:
            current_index = profiles.index(current) if current else -1
        except ValueError:
            current_index = -1
        next_profile = profiles[(current_index + 1) % len(profiles)]
        self.apply_profile(next_profile)

    def apply_profile(self, profile: str | None) -> None:
        if not profile:
            return
        self.profiles = (profile,)
        self.all_profiles = False
        self.contexts = []
        self.query_one("#status", Static).update(f"Switched profile to {profile}.")
        self.action_refresh()


class ProcessingJobsApp(BaseSageMakerApp):
    TITLE = "SageMaker Processing Jobs"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        Binding("left", "previous_job", "Previous", priority=True),
        Binding("up", "previous_job", "Previous", priority=True),
        Binding("right", "next_job", "Next", priority=True),
        Binding("down", "next_job", "Next", priority=True),
        Binding("s", "submit_processing", "Submit", priority=True),
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
                "Use arrows to move, P to switch profile, s to submit, r to refresh, q to quit."
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

    def action_submit_processing(self) -> None:
        self.push_screen(ProcessingSubmitScreen(), self.submit_processing_from_form)

    def submit_processing_from_form(self, config_path: str | None) -> None:
        if not config_path:
            return
        try:
            ctx = self.primary_context()
            if ctx is None:
                raise AwsCliError("No AWS context available.")
            spec = load_job_spec(Path(config_path).expanduser())
            response = submit_processing_job(ctx, spec)
            job_name = spec.get("ProcessingJobName") or response.get("ProcessingJobArn", "")
            self.query_one("#status", Static).update(f"Submitted processing job {job_name} on {ctx.profile}/{ctx.region}.")
            self.contexts = []
            self.action_refresh()
        except Exception as exc:
            self.show_error(exc)

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
        Binding("s", "start_pipeline", "Start", priority=True),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
        pipeline_name: str | None,
        recent_hours: int = 3,
    ) -> None:
        super().__init__(profiles, region, all_profiles, refresh_seconds)
        self.pipeline_name = pipeline_name
        self.recent_hours = recent_hours
        self.executions: list[tuple[AwsContext, PipelineExecutionView]] = []
        self.steps: list[dict[str, Any]] = []
        self.selected_context: AwsContext | None = None
        self.selected_execution_arn: str | None = None
        self.loaded_log_step_key: tuple[str, str] | None = None
        self._rendering_executions = False
        self._updating_steps = False
        self._suppress_next_execution_highlight = False
        self._suppress_next_step_highlight = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Vertical(id="pipeline-content"):
            with Vertical(id="executions-pane"):
                executions = DataTable(id="executions")
                executions.cursor_type = "row"
                yield executions
            with Horizontal(id="bottom-pane"):
                with Vertical(id="steps-pane"):
                    steps = DataTable(id="steps")
                    steps.cursor_type = "row"
                    yield steps
                with Vertical(id="logs-pane"):
                    yield RichLog(id="logs", wrap=True, highlight=True, auto_scroll=False)
        yield Footer()

    def on_mount(self) -> None:
        executions = self.query_one("#executions", DataTable)
        executions.add_columns("Profile", "Region", "Pipeline", "Execution", "Status", "Runtime")
        steps = self.query_one("#steps", DataTable)
        steps.add_columns("Step", "Type", "Status", "Runtime", "Failure")
        super().on_mount()

    def action_refresh(self) -> None:
        previous_execution_arn = self.selected_execution_arn
        previous_step_name = self.selected_step_name()
        try:
            pairs: list[tuple[AwsContext, PipelineExecutionView]] = []
            for ctx in self.load_contexts():
                for execution in list_active_pipeline_executions(
                    ctx, pipeline_name=self.pipeline_name, recent_hours=self.recent_hours
                ):
                    pairs.append((ctx, execution))
            self.executions = pairs
            self.render_executions(previous_execution_arn, previous_step_name, preserve_logs=True)
            self.query_one("#status", Static).update(
                f"{len(self.executions)} active/recent pipeline execution(s), window={self.recent_hours}h. "
                "Use left/right to switch panes, P to switch profile, s to start, l to load logs."
            )
        except AwsCliError as exc:
            self.show_error(exc)

    def action_focus_executions(self) -> None:
        self.query_one("#executions", DataTable).focus()

    def action_focus_steps(self) -> None:
        self.query_one("#steps", DataTable).focus()

    def action_load_logs(self) -> None:
        self.load_selected_step_logs()

    def action_start_pipeline(self) -> None:
        self.push_screen(PipelineStartScreen(self.pipeline_name or ""), self.start_pipeline_from_form)

    def start_pipeline_from_form(self, values: dict[str, str] | None) -> None:
        if not values:
            return
        try:
            ctx = self.selected_context or self.primary_context()
            if ctx is None:
                raise AwsCliError("No AWS context available.")
            pipeline_name = values.get("pipeline_name", "").strip()
            if not pipeline_name:
                raise AwsCliError("Pipeline name is required.")
            response = start_pipeline_execution(
                ctx,
                pipeline_name=pipeline_name,
                display_name=values.get("display_name") or None,
                parameters=parse_parameters(_split_parameters(values.get("parameters", ""))),
                client_request_token=None,
            )
            execution_arn = response.get("PipelineExecutionArn", "")
            self.query_one("#status", Static).update(f"Started pipeline {pipeline_name} on {ctx.profile}/{ctx.region}.")
            self.contexts = []
            self.action_refresh()
            if execution_arn:
                self.render_executions(execution_arn, preserve_logs=False)
        except Exception as exc:
            self.show_error(exc)

    def render_executions(
        self,
        preferred_execution_arn: str | None = None,
        preferred_step_name: str | None = None,
        preserve_logs: bool = False,
    ) -> None:
        table = self.query_one("#executions", DataTable)
        selected_index = 0 if self.executions else None
        self._rendering_executions = True
        try:
            table.clear()
            for index, (_, execution) in enumerate(self.executions):
                if preferred_execution_arn and execution.execution_arn == preferred_execution_arn:
                    selected_index = index
                table.add_row(
                    execution.profile,
                    execution.region,
                    execution.pipeline_name,
                    execution.display_name or execution.execution_arn.rsplit("/", 1)[-1],
                    execution.status,
                    format_duration(execution.start_time),
                    key=str(index),
                )
            if selected_index is not None and table.row_count:
                self._suppress_next_execution_highlight = True
                table.move_cursor(row=selected_index, scroll=False)
        finally:
            self._rendering_executions = False
        self.update_steps(selected_index, preferred_step_name=preferred_step_name, preserve_logs=preserve_logs)

    @on(DataTable.RowHighlighted, "#executions")
    def on_execution_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self._rendering_executions or self._suppress_next_execution_highlight:
            self._suppress_next_execution_highlight = False
            return
        try:
            self.update_steps(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    @on(DataTable.RowHighlighted, "#steps")
    def on_step_highlighted(self, _: DataTable.RowHighlighted) -> None:
        if self._updating_steps or self._suppress_next_step_highlight:
            self._suppress_next_step_highlight = False
            return
        if self.loaded_log_step_key == self.selected_step_key():
            return
        self.loaded_log_step_key = None
        self.render_step_failure_or_hint()

    def update_steps(
        self,
        index: int | None,
        preferred_step_name: str | None = None,
        preserve_logs: bool = False,
    ) -> None:
        self.steps = []
        self.selected_context = None
        self.selected_execution_arn = None
        logs = self.query_one("#logs", RichLog)
        if index is None or index >= len(self.executions):
            self.query_one("#steps", DataTable).clear()
            self.loaded_log_step_key = None
            logs.clear()
            logs.write(f"No active or recent pipeline executions in the last {self.recent_hours}h.")
            return

        ctx, execution = self.executions[index]
        self.selected_context = ctx
        self.selected_execution_arn = execution.execution_arn
        try:
            self.steps = list_pipeline_steps(ctx, execution.execution_arn)
        except AwsCliError as exc:
            self.loaded_log_step_key = None
            logs.clear()
            logs.write(str(exc))
            return
        table = self.query_one("#steps", DataTable)
        selected_step_index = 0 if self.steps else None
        self._updating_steps = True
        try:
            table.clear()
            for step_index, step in enumerate(self.steps):
                if preferred_step_name and step.get("StepName", "") == preferred_step_name:
                    selected_step_index = step_index
                table.add_row(
                    step.get("StepName", ""),
                    step.get("StepType", ""),
                    step.get("StepStatus", ""),
                    format_duration(step.get("StartTime"), step.get("EndTime")),
                    _shorten(step.get("FailureReason", ""), 80),
                    key=str(step_index),
                )
            if selected_step_index is not None and table.row_count:
                self._suppress_next_step_highlight = True
                table.move_cursor(row=selected_step_index, scroll=False)
        finally:
            self._updating_steps = False
        if preserve_logs and self.loaded_log_step_key == self.selected_step_key():
            return
        self.loaded_log_step_key = None
        self.render_step_failure_or_hint()

    def render_step_failure_or_hint(self) -> None:
        if self.loaded_log_step_key == self.selected_step_key():
            return
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
            self.loaded_log_step_key = None
            logs.write("Select a pipeline step first.")
            return
        self.loaded_log_step_key = self.selected_step_key()
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

    def selected_step_name(self) -> str | None:
        step = self.selected_step()
        if step is None:
            return None
        return step.get("StepName", "")

    def selected_step_key(self) -> tuple[str, str] | None:
        step_name = self.selected_step_name()
        if not self.selected_execution_arn or not step_name:
            return None
        return self.selected_execution_arn, step_name


class SmopsTuiApp(App[str | None]):
    TITLE = "smops"
    CSS = BaseSageMakerApp.CSS
    BINDINGS = [
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Choose a SageMaker view. Press Enter to open.", id="status")
        table = DataTable(id="home")
        table.cursor_type = "row"
        yield table
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#home", DataTable)
        table.add_columns("View", "Description")
        table.add_row("pipelines", "Pipeline executions, steps, failed logs, start pipeline", key="pipelines")
        table.add_row("processing", "Processing jobs, details, submit processing job", key="processing")
        table.focus()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.exit(str(event.row_key.value))


class PipelineStartScreen(ModalScreen[dict[str, str] | None]):
    def __init__(self, pipeline_name: str = "") -> None:
        super().__init__()
        self.pipeline_name = pipeline_name

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog"):
            yield Label("Start SageMaker Pipeline")
            yield Input(value=self.pipeline_name, placeholder="Pipeline name", id="pipeline-name")
            yield Input(placeholder="Display name (optional)", id="display-name")
            yield Input(placeholder="Parameters: Name=Value,Other=Value", id="parameters")
            with Horizontal():
                yield Button("Start", id="start", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self) -> None:
        self.query_one("#pipeline-name", Input).focus()

    @on(Button.Pressed, "#start")
    def start(self) -> None:
        self.dismiss(
            {
                "pipeline_name": self.query_one("#pipeline-name", Input).value,
                "display_name": self.query_one("#display-name", Input).value,
                "parameters": self.query_one("#parameters", Input).value,
            }
        )

    @on(Button.Pressed, "#cancel")
    def cancel(self) -> None:
        self.dismiss(None)


class ProcessingSubmitScreen(ModalScreen[str | None]):
    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog"):
            yield Label("Submit SageMaker Processing Job")
            yield Input(placeholder="Config path (.json/.yaml)", id="config-path")
            with Horizontal():
                yield Button("Submit", id="submit", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self) -> None:
        self.query_one("#config-path", Input).focus()

    @on(Button.Pressed, "#submit")
    def submit(self) -> None:
        self.dismiss(self.query_one("#config-path", Input).value)

    @on(Button.Pressed, "#cancel")
    def cancel(self) -> None:
        self.dismiss(None)


def _split_parameters(value: str) -> list[str]:
    if not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _shorten(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"

