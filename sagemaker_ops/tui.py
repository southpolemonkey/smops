from __future__ import annotations

from pathlib import Path
from typing import Any

from textual import events, on
from textual.binding import Binding
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, RichLog, Static
from textual.worker import Worker, WorkerState

PROFILE_PICKER_VISIBLE_ROWS = 16


from sagemaker_ops.aws import (
    AirflowDagView,
    AirflowDagRunView,
    AirflowPoolView,
    AirflowTaskInstanceView,
    AwsCliError,
    AwsContext,
    EcsClusterView,
    EcsServiceView,
    EcsTaskView,
    available_profiles,
    ProcessingJobView,
    PipelineExecutionView,
    build_contexts,
    format_dt,
    format_duration,
    infer_log_source,
    list_active_pipeline_executions,
    list_airflow_dag_runs,
    list_airflow_dags,
    list_airflow_pools,
    list_airflow_task_instances,
    list_ecs_clusters,
    list_ecs_services,
    list_ecs_tasks,
    list_pipeline_steps,
    list_processing_jobs,
    load_job_spec,
    parse_parameters,
    start_pipeline_execution,
    submit_processing_job,
    tail_ecs_task_logs,
    tail_processing_job_logs,
    tail_step_logs,
    trigger_airflow_dag,
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

    #detail, #logs, #processing-logs {
        height: 1fr;
        border: solid $surface;
        padding: 1;
    }

    #jobs {
        width: 52%;
    }

    #processing-side {
        width: 48%;
    }

    #detail {
        height: 45%;
    }

    #processing-logs {
        height: 55%;
    }

    #pipeline-content, #ecs-content {
        height: 1fr;
    }

    #ecs-top {
        height: 42%;
    }

    #ecs-bottom {
        height: 58%;
    }

    #ecs-clusters-pane {
        width: 36%;
    }

    #ecs-services-pane {
        width: 64%;
    }

    #ecs-tasks-pane {
        width: 52%;
    }

    #ecs-logs-pane {
        width: 48%;
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

    #airflow-content {
        height: 1fr;
    }

    #airflow-top {
        height: 42%;
    }

    #airflow-bottom {
        height: 58%;
    }

    #airflow-runs-pane {
        width: 50%;
    }

    #airflow-detail-pane {
        width: 50%;
    }

    #airflow-search {
        display: none;
        margin: 0 1;
        border: tall $accent;
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

    #profiles {
        height: 16;
        margin-bottom: 1;
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
        if isinstance(self.screen, ProfileSelectScreen):
            if event.key in {"down", "j"}:
                event.stop()
                self.screen.action_next_profile()
                return
            if event.key in {"up", "k"}:
                event.stop()
                self.screen.action_previous_profile()
                return
            if event.key == "enter":
                event.stop()
                self.screen.action_select_profile()
                return
            if event.key == "escape":
                event.stop()
                self.screen.action_cancel()
                return
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
        self.push_screen(ProfileSelectScreen(profiles, self.current_profile()), self.apply_profile)

    def apply_profile(self, profile: str | None, contexts: list[AwsContext] | None = None) -> None:
        if not profile:
            return
        try:
            self.contexts = contexts or build_contexts((profile,), self.region, all_profiles=False)
            self.profiles = (profile,)
            self.all_profiles = False
            self.query_one("#status", Static).update(f"Switched profile to {profile}.")
            self.action_refresh()
        except AwsCliError as exc:
            self.contexts = []
            self.show_error(exc)

    def current_profile(self) -> str | None:
        if self.all_profiles:
            return None
        return self.profiles[0] if self.profiles else None

    def profile_label(self) -> str:
        if self.all_profiles:
            return "all profiles"
        profile = self.current_profile()
        return profile or "default"


class ProcessingJobsApp(BaseSageMakerApp):
    TITLE = "SageMaker Processing Jobs"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        Binding("left", "previous_job", "Previous", priority=True),
        Binding("up", "previous_job", "Previous", priority=True),
        Binding("right", "next_job", "Next", priority=True),
        Binding("down", "next_job", "Next", priority=True),
        Binding("l", "load_logs", "Logs", priority=True),
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
        self.loaded_processing_log_key: tuple[str, str, str] | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Horizontal(id="content"):
            table = DataTable(id="jobs")
            table.cursor_type = "row"
            yield table
            with Vertical(id="processing-side"):
                yield Static("", id="detail")
                yield RichLog(id="processing-logs", wrap=True, highlight=True, auto_scroll=False)
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
                f"Profile: {self.profile_label()}. "
                "Use arrows to move, P to choose profile, l to load logs, s to submit, r to refresh, q to quit."
            )
        except AwsCliError as exc:
            self.show_error(exc)

    def action_previous_job(self) -> None:
        if isinstance(self.screen, ProfileSelectScreen):
            self.screen.action_previous_profile()
            return
        table = self.query_one("#jobs", DataTable)
        if table.row_count:
            table.move_cursor(row=max(0, table.cursor_row - 1))

    def action_next_job(self) -> None:
        if isinstance(self.screen, ProfileSelectScreen):
            self.screen.action_next_profile()
            return
        table = self.query_one("#jobs", DataTable)
        if table.row_count:
            table.move_cursor(row=min(table.row_count - 1, table.cursor_row + 1))

    def action_load_logs(self) -> None:
        self.load_selected_processing_logs()

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
        logs = self.query_one("#processing-logs", RichLog)
        if index is None or index >= len(self.jobs):
            detail.update("No running processing jobs.")
            self.loaded_processing_log_key = None
            logs.clear()
            logs.write("Select a processing job to view logs.")
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
        if self.loaded_processing_log_key == self.selected_processing_log_key():
            return
        self.loaded_processing_log_key = None
        logs.clear()
        logs.write("Press l to load CloudWatch log tail.")

    def selected_processing_job(self) -> ProcessingJobView | None:
        table = self.query_one("#jobs", DataTable)
        if not self.jobs or table.cursor_row >= len(self.jobs):
            return None
        return self.jobs[table.cursor_row]

    def selected_processing_context(self) -> AwsContext | None:
        job = self.selected_processing_job()
        if job is None:
            return None
        for ctx in self.load_contexts():
            if ctx.profile == job.profile and ctx.region == job.region:
                return ctx
        return None

    def selected_processing_log_key(self) -> tuple[str, str, str] | None:
        job = self.selected_processing_job()
        if job is None:
            return None
        return job.profile, job.region, job.name

    def load_selected_processing_logs(self) -> None:
        logs = self.query_one("#processing-logs", RichLog)
        logs.clear()
        job = self.selected_processing_job()
        ctx = self.selected_processing_context()
        if job is None or ctx is None:
            self.loaded_processing_log_key = None
            logs.write("Select a processing job first.")
            return
        self.loaded_processing_log_key = self.selected_processing_log_key()
        logs.write(f"CloudWatch /aws/sagemaker/ProcessingJobs prefix={job.name}")
        for line in tail_processing_job_logs(ctx, job.name):
            logs.write(line)


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
        self._refresh_previous_execution_arn: str | None = None
        self._refresh_previous_step_name: str | None = None

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
        self.query_one("#logs", RichLog).write("Loading pipeline executions...")
        self.set_interval(self.refresh_seconds, self.action_refresh)
        self.call_after_refresh(self.action_refresh)

    def action_refresh(self) -> None:
        self._refresh_previous_execution_arn = self.selected_execution_arn
        self._refresh_previous_step_name = self.selected_step_name()
        self.query_one("#status", Static).update(
            f"Loading pipeline executions, window={self.recent_hours}h. "
            f"Profile: {self.profile_label()}."
        )
        self.run_worker(
            self._load_pipeline_execution_pairs,
            name="pipeline-refresh",
            group="pipeline-refresh",
            exclusive=True,
            thread=True,
            exit_on_error=False,
        )

    def _load_pipeline_execution_pairs(self) -> list[tuple[AwsContext, PipelineExecutionView]]:
        pairs: list[tuple[AwsContext, PipelineExecutionView]] = []
        for ctx in self.load_contexts():
            for execution in list_active_pipeline_executions(
                ctx, pipeline_name=self.pipeline_name, recent_hours=self.recent_hours
            ):
                pairs.append((ctx, execution))
        return pairs

    @on(Worker.StateChanged)
    def on_pipeline_refresh_worker_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "pipeline-refresh" or event.state not in {WorkerState.SUCCESS, WorkerState.ERROR}:
            return
        if event.state == WorkerState.ERROR:
            exc = event.worker.error
            self.show_error(exc if isinstance(exc, Exception) else AwsCliError(str(exc)))
            return
        self.executions = event.worker.result
        self.render_executions(
            self._refresh_previous_execution_arn,
            self._refresh_previous_step_name,
            preserve_logs=True,
        )
        self.query_one("#status", Static).update(
            f"{len(self.executions)} active/recent pipeline execution(s), window={self.recent_hours}h. "
            f"Profile: {self.profile_label()}. "
            "Use left/right to switch panes, P to choose profile, s to start, l to load logs."
        )

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


class EcsTasksApp(BaseSageMakerApp):
    TITLE = "Amazon ECS Tasks"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        ("left", "focus_left", "Left"),
        ("right", "focus_right", "Right"),
        ("l", "load_logs", "Logs"),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
    ) -> None:
        super().__init__(profiles, region, all_profiles, refresh_seconds)
        self.clusters: list[tuple[AwsContext, EcsClusterView]] = []
        self.services: list[EcsServiceView] = []
        self.tasks: list[EcsTaskView] = []
        self.selected_context: AwsContext | None = None
        self.selected_cluster: EcsClusterView | None = None
        self.loaded_ecs_log_key: tuple[str, str, str, str] | None = None
        self._rendering_clusters = False
        self._rendering_services = False
        self._rendering_tasks = False
        # Saved before each background refresh to restore cursor position after.
        self._refresh_previous_cluster_name: str | None = None
        self._refresh_previous_service_name: str | None = None
        self._refresh_previous_focused_id: str | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Vertical(id="ecs-content"):
            with Horizontal(id="ecs-top"):
                with Vertical(id="ecs-clusters-pane"):
                    clusters = DataTable(id="ecs-clusters")
                    clusters.cursor_type = "row"
                    yield clusters
                with Vertical(id="ecs-services-pane"):
                    services = DataTable(id="ecs-services")
                    services.cursor_type = "row"
                    yield services
            with Horizontal(id="ecs-bottom"):
                with Vertical(id="ecs-tasks-pane"):
                    tasks = DataTable(id="ecs-tasks")
                    tasks.cursor_type = "row"
                    yield tasks
                with Vertical(id="ecs-logs-pane"):
                    yield RichLog(id="ecs-logs", wrap=True, highlight=True, auto_scroll=False)
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#ecs-clusters", DataTable).add_columns("Profile", "Region", "Cluster", "Running", "Services")
        self.query_one("#ecs-services", DataTable).add_columns("Service", "Status", "Desired", "Running", "Pending")
        self.query_one("#ecs-tasks", DataTable).add_columns("Task", "Last", "Desired", "Launch", "Started", "Containers")
        super().on_mount()

    def action_refresh(self) -> None:
        # Save current selection and focus so we can restore after the worker completes.
        self._refresh_previous_cluster_name = self.selected_cluster.cluster_name if self.selected_cluster else None
        self._refresh_previous_service_name = (
            self.services[self.query_one("#ecs-services", DataTable).cursor_row].service_name
            if self.services and self.query_one("#ecs-services", DataTable).row_count
            else None
        )
        self._refresh_previous_focused_id = self.focused.id if self.focused else None
        self.query_one("#status", Static).update(
            f"Loading ECS clusters... Profile: {self.profile_label()}."
        )
        self.run_worker(
            self._load_clusters,
            name="ecs-refresh",
            group="ecs-refresh",
            exclusive=True,
            thread=True,
            exit_on_error=False,
        )

    def _load_clusters(self) -> list[tuple[AwsContext, EcsClusterView]]:
        return [(ctx, cluster) for ctx in self.load_contexts() for cluster in list_ecs_clusters(ctx)]

    @on(Worker.StateChanged)
    def on_ecs_refresh_worker_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name != "ecs-refresh" or event.state not in {WorkerState.SUCCESS, WorkerState.ERROR}:
            return
        if event.state == WorkerState.ERROR:
            exc = event.worker.error
            self.show_error(exc if isinstance(exc, Exception) else AwsCliError(str(exc)))
            return
        self.clusters = event.worker.result
        self.render_clusters(
            preferred_cluster_name=self._refresh_previous_cluster_name,
            preferred_service_name=self._refresh_previous_service_name,
            preferred_focused_id=self._refresh_previous_focused_id,
        )
        self.query_one("#status", Static).update(
            f"{len(self.clusters)} ECS cluster(s). Refresh every {self.refresh_seconds}s. "
            f"Profile: {self.profile_label()}. Use left/right to switch panes, arrows to move, l to load logs."
        )

    def action_focus_left(self) -> None:
        focused = self.focused
        if focused and focused.id == "ecs-services":
            self.query_one("#ecs-clusters", DataTable).focus()
        elif focused and focused.id == "ecs-tasks":
            self.query_one("#ecs-services", DataTable).focus()
        else:
            self.query_one("#ecs-tasks", DataTable).focus()

    def action_focus_right(self) -> None:
        focused = self.focused
        if focused and focused.id == "ecs-clusters":
            self.query_one("#ecs-services", DataTable).focus()
        elif focused and focused.id == "ecs-services":
            self.query_one("#ecs-tasks", DataTable).focus()
        else:
            self.query_one("#ecs-clusters", DataTable).focus()

    def action_load_logs(self) -> None:
        self.load_selected_ecs_logs()

    def render_clusters(
        self,
        preferred_cluster_name: str | None = None,
        preferred_service_name: str | None = None,
        preferred_focused_id: str | None = None,
    ) -> None:
        table = self.query_one("#ecs-clusters", DataTable)
        selected_index = 0 if self.clusters else None
        self._rendering_clusters = True
        try:
            table.clear()
            for index, (_, cluster) in enumerate(self.clusters):
                if preferred_cluster_name and cluster.cluster_name == preferred_cluster_name:
                    selected_index = index
                table.add_row(
                    cluster.profile,
                    cluster.region,
                    cluster.cluster_name or cluster.cluster_arn,
                    str(cluster.running_tasks),
                    str(cluster.active_services),
                    key=str(index),
                )
            if selected_index is not None and table.row_count:
                table.move_cursor(row=selected_index, scroll=False)
        finally:
            self._rendering_clusters = False
        self.update_services(selected_index, preferred_service_name=preferred_service_name)
        # Restore focus to whichever pane the user was in before the refresh.
        if preferred_focused_id:
            try:
                self.query_one(f"#{preferred_focused_id}").focus()
            except Exception:
                pass

    @on(DataTable.RowHighlighted, "#ecs-clusters")
    def on_ecs_cluster_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self._rendering_clusters:
            return
        try:
            self.update_services(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    def update_services(self, index: int | None, preferred_service_name: str | None = None) -> None:
        self.services = []
        self.tasks = []
        self.selected_context = None
        self.selected_cluster = None
        logs = self.query_one("#ecs-logs", RichLog)
        logs.clear()
        if index is None or index >= len(self.clusters):
            self.query_one("#ecs-services", DataTable).clear()
            self.query_one("#ecs-tasks", DataTable).clear()
            logs.write("No ECS clusters found.")
            return
        ctx, cluster = self.clusters[index]
        self.selected_context = ctx
        self.selected_cluster = cluster
        try:
            self.services = list_ecs_services(ctx, cluster.cluster_name or cluster.cluster_arn)
        except AwsCliError as exc:
            logs.write(str(exc))
            return
        table = self.query_one("#ecs-services", DataTable)
        selected_service_index = 0 if self.services else None
        self._rendering_services = True
        try:
            table.clear()
            for service_index, service in enumerate(self.services):
                if preferred_service_name and service.service_name == preferred_service_name:
                    selected_service_index = service_index
                table.add_row(
                    service.service_name or service.service_arn,
                    service.status,
                    str(service.desired_count),
                    str(service.running_count),
                    str(service.pending_count),
                    key=str(service_index),
                )
            if selected_service_index is not None and table.row_count:
                table.move_cursor(row=selected_service_index, scroll=False)
        finally:
            self._rendering_services = False
        self.update_tasks(selected_service_index)

    @on(DataTable.RowHighlighted, "#ecs-services")
    def on_ecs_service_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self._rendering_services:
            return
        try:
            self.update_tasks(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    def update_tasks(self, service_index: int | None) -> None:
        self.tasks = []
        logs = self.query_one("#ecs-logs", RichLog)
        logs.clear()
        cluster = self.selected_cluster
        ctx = self.selected_context
        if cluster is None or ctx is None:
            self.query_one("#ecs-tasks", DataTable).clear()
            logs.write("Select an ECS cluster first.")
            return
        service_name = None
        if service_index is not None and service_index < len(self.services):
            service_name = self.services[service_index].service_name
        try:
            self.tasks = list_ecs_tasks(ctx, cluster.cluster_name or cluster.cluster_arn, service=service_name, desired_status="RUNNING")
        except AwsCliError as exc:
            logs.write(str(exc))
            return
        table = self.query_one("#ecs-tasks", DataTable)
        self._rendering_tasks = True
        try:
            table.clear()
            for task_index, task in enumerate(self.tasks):
                table.add_row(
                    task.task_id,
                    task.last_status,
                    task.desired_status,
                    task.launch_type,
                    format_dt(task.started_at),
                    ", ".join(container.get("name", "") for container in task.containers),
                    key=str(task_index),
                )
            if table.row_count:
                table.move_cursor(row=0, scroll=False)
        finally:
            self._rendering_tasks = False
        self.loaded_ecs_log_key = None
        logs.write("Press l to load CloudWatch logs for the selected running task.")

    @on(DataTable.RowHighlighted, "#ecs-tasks")
    def on_ecs_task_highlighted(self, _: DataTable.RowHighlighted) -> None:
        if self._rendering_tasks:
            return
        if self.loaded_ecs_log_key == self.selected_ecs_log_key():
            return
        self.loaded_ecs_log_key = None
        logs = self.query_one("#ecs-logs", RichLog)
        logs.clear()
        logs.write("Press l to load CloudWatch logs for the selected running task.")

    def selected_ecs_task(self) -> EcsTaskView | None:
        table = self.query_one("#ecs-tasks", DataTable)
        if not self.tasks or table.cursor_row >= len(self.tasks):
            return None
        return self.tasks[table.cursor_row]

    def selected_ecs_log_key(self) -> tuple[str, str, str, str] | None:
        ctx = self.selected_context
        cluster = self.selected_cluster
        task = self.selected_ecs_task()
        if ctx is None or cluster is None or task is None:
            return None
        return ctx.profile, ctx.region, cluster.cluster_name or cluster.cluster_arn, task.task_arn

    def load_selected_ecs_logs(self) -> None:
        logs = self.query_one("#ecs-logs", RichLog)
        logs.clear()
        ctx = self.selected_context
        cluster = self.selected_cluster
        task = self.selected_ecs_task()
        if ctx is None or cluster is None or task is None:
            self.loaded_ecs_log_key = None
            logs.write("Select an ECS task first.")
            return
        cluster_name = cluster.cluster_name or cluster.cluster_arn
        self.loaded_ecs_log_key = self.selected_ecs_log_key()
        logs.write(f"CloudWatch logs for task {task.task_id}")
        for line in tail_ecs_task_logs(ctx, cluster_name, task.task_arn):
            logs.write(line)


class AirflowApp(BaseSageMakerApp):
    TITLE = "Amazon MWAA / Airflow"
    BINDINGS = BaseSageMakerApp.BINDINGS + [
        ("left", "focus_left", "Left"),
        ("right", "focus_right", "Right"),
        ("l", "load_tasks", "Tasks"),
        Binding("t", "trigger_dag", "Trigger", priority=True),
        Binding("/", "search_dags", "Search", priority=True),
        Binding("escape", "clear_search", "Clear search", show=False, priority=True),
    ]

    def __init__(
        self,
        profiles: tuple[str, ...],
        region: str | None,
        all_profiles: bool,
        refresh_seconds: int,
        environment: str | None,
    ) -> None:
        super().__init__(profiles, region, all_profiles, refresh_seconds)
        self.environment = environment
        self.dags: list[AirflowDagView] = []
        # DAGs currently shown after applying the fuzzy search filter. All row
        # operations index into this list, not self.dags.
        self.filtered_dags: list[AirflowDagView] = []
        self.search_query = ""
        self.searching = False
        self.pools: list[AirflowPoolView] = []
        self.pools_error: str | None = None
        self.runs: list[AirflowDagRunView] = []
        self.selected_context: AwsContext | None = None
        self._rendering_dags = False
        self._rendering_runs = False
        # Keys identify which async result is current, so stale worker results are ignored.
        self._requested_runs_dag: str | None = None
        self._requested_tasks_key: tuple[str, str] | None = None
        self._refresh_previous_dag_id: str | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading...", id="status")
        with Vertical(id="airflow-content"):
            with Vertical(id="airflow-top"):
                yield Input(placeholder="Fuzzy search DAGs — Enter to jump, Esc to clear", id="airflow-search")
                dags = DataTable(id="airflow-dags")
                dags.cursor_type = "row"
                yield dags
            with Horizontal(id="airflow-bottom"):
                with Vertical(id="airflow-runs-pane"):
                    runs = DataTable(id="airflow-runs")
                    runs.cursor_type = "row"
                    yield runs
                with Vertical(id="airflow-detail-pane"):
                    yield RichLog(id="airflow-detail", wrap=True, highlight=True, auto_scroll=False)
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#airflow-dags", DataTable).add_columns("Profile", "Environment", "DAG", "Paused", "Schedule")
        self.query_one("#airflow-runs", DataTable).add_columns("Run", "State", "Type", "Runtime", "Started")
        self.query_one("#airflow-detail", RichLog).write("Loading MWAA DAGs...")
        super().on_mount()

    def action_refresh(self) -> None:
        if not self.environment:
            self.show_error(
                AwsCliError("No MWAA environment set. Launch with --env or run 'smops config set-mwaa-env <name>'.")
            )
            return
        self._refresh_previous_dag_id = self.selected_dag().dag_id if self.selected_dag() else None
        self.query_one("#status", Static).update(
            f"Loading MWAA {self.environment}... Profile: {self.profile_label()}."
        )
        self.run_worker(
            self._load_dags_and_pools,
            name="airflow-refresh",
            group="airflow-refresh",
            exclusive=True,
            thread=True,
            exit_on_error=False,
        )

    def _load_dags_and_pools(
        self,
    ) -> tuple[AwsContext | None, list[AirflowDagView], list[AirflowPoolView], str | None]:
        ctx = self.primary_context()
        if ctx is None or not self.environment:
            return None, [], [], None
        dags = list_airflow_dags(ctx, self.environment)
        # Pools require a separate Airflow RBAC permission; a role that can read
        # DAGs may still get 403 on pools. Degrade gracefully so the DAG/run view
        # keeps working instead of failing the whole refresh.
        pools: list[AirflowPoolView] = []
        pools_error: str | None = None
        try:
            pools = list_airflow_pools(ctx, self.environment)
        except AwsCliError as exc:
            pools_error = str(exc)
        return ctx, dags, pools, pools_error

    def _load_runs(self) -> tuple[str, list[AirflowDagRunView]]:
        dag = self.selected_dag()
        ctx = self.selected_context
        if dag is None or ctx is None or not self.environment:
            return "", []
        return dag.dag_id, list_airflow_dag_runs(ctx, self.environment, dag.dag_id)

    def _load_tasks(self) -> tuple[tuple[str, str] | None, list[AirflowTaskInstanceView]]:
        dag = self.selected_dag()
        run = self.selected_run()
        ctx = self.selected_context
        if dag is None or run is None or ctx is None or not self.environment:
            return None, []
        key = (dag.dag_id, run.dag_run_id)
        return key, list_airflow_task_instances(ctx, self.environment, dag.dag_id, run.dag_run_id)

    @on(Worker.StateChanged)
    def on_airflow_worker_changed(self, event: Worker.StateChanged) -> None:
        name = event.worker.name
        if name not in {"airflow-refresh", "airflow-runs", "airflow-tasks"}:
            return
        if event.state == WorkerState.ERROR:
            exc = event.worker.error
            self.show_error(exc if isinstance(exc, Exception) else AwsCliError(str(exc)))
            return
        if event.state != WorkerState.SUCCESS:
            return
        if name == "airflow-refresh":
            ctx, dags, pools, pools_error = event.worker.result
            self.selected_context = ctx
            self.dags = dags
            self.pools = pools
            self.pools_error = pools_error
            self.render_dags(self._refresh_previous_dag_id)
            self.render_pools()
            self.query_one("#status", Static).update(
                f"{len(self.dags)} DAG(s) in {self.environment}. Refresh every {self.refresh_seconds}s. "
                f"Profile: {self.profile_label()}. Use arrows to move, / to search, l to load tasks, t to trigger."
            )
        elif name == "airflow-runs":
            dag_id, runs = event.worker.result
            if dag_id != self._requested_runs_dag:
                return
            self.runs = runs
            self.render_runs()
        elif name == "airflow-tasks":
            key, instances = event.worker.result
            if key != self._requested_tasks_key:
                return
            self.render_tasks(instances)

    def action_focus_left(self) -> None:
        focused = self.focused
        if focused and focused.id == "airflow-runs":
            self.query_one("#airflow-dags", DataTable).focus()
        else:
            self.query_one("#airflow-runs", DataTable).focus()

    def action_focus_right(self) -> None:
        focused = self.focused
        if focused and focused.id == "airflow-dags":
            self.query_one("#airflow-runs", DataTable).focus()
        else:
            self.query_one("#airflow-dags", DataTable).focus()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool:
        # While the search box is focused, let it own text keys. Only search-control
        # actions stay live so single-letter bindings (r/l/t/p/q/s) don't fire as
        # the user types a DAG name.
        if self.searching and action not in {"clear_search", "search_dags"}:
            return False
        return True

    def on_key(self, event) -> None:
        # The base class grabs p/P/s as raw keys (not bindings), which would hijack
        # typing in the search box. Let the focused Input consume everything except
        # Escape while searching.
        if self.searching:
            if event.key == "escape":
                event.stop()
                self.action_clear_search()
            return
        super().on_key(event)

    def action_search_dags(self) -> None:
        self.searching = True
        search = self.query_one("#airflow-search", Input)
        search.styles.display = "block"
        search.value = self.search_query
        search.focus()

    def action_clear_search(self) -> None:
        if not self.searching and not self.search_query:
            return
        self.searching = False
        self.search_query = ""
        search = self.query_one("#airflow-search", Input)
        search.value = ""
        search.styles.display = "none"
        self.render_dags()
        self.query_one("#airflow-dags", DataTable).focus()

    @on(Input.Changed, "#airflow-search")
    def on_search_changed(self, event: Input.Changed) -> None:
        self.search_query = event.value
        self.render_dags()

    @on(Input.Submitted, "#airflow-search")
    def on_search_submitted(self, _: Input.Submitted) -> None:
        # Jump to the current top match and return focus to the table.
        self.searching = False
        self.query_one("#airflow-search", Input).styles.display = "none"
        table = self.query_one("#airflow-dags", DataTable)
        if table.row_count:
            table.move_cursor(row=0, scroll=True)
        table.focus()

    def render_dags(self, preferred_dag_id: str | None = None) -> None:
        table = self.query_one("#airflow-dags", DataTable)
        self.filtered_dags = _fuzzy_filter_dags(self.dags, self.search_query)
        selected_index = 0 if self.filtered_dags else None
        self._rendering_dags = True
        try:
            table.clear()
            for index, dag in enumerate(self.filtered_dags):
                if preferred_dag_id and dag.dag_id == preferred_dag_id:
                    selected_index = index
                table.add_row(
                    dag.profile,
                    dag.environment,
                    dag.dag_id,
                    "yes" if dag.is_paused else "no",
                    dag.schedule_interval,
                    key=str(index),
                )
            if selected_index is not None and table.row_count:
                table.move_cursor(row=selected_index, scroll=False)
        finally:
            self._rendering_dags = False
        self.update_runs(selected_index)

    @on(DataTable.RowHighlighted, "#airflow-dags")
    def on_dag_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self._rendering_dags:
            return
        try:
            self.update_runs(int(str(event.row_key.value)))
        except (TypeError, ValueError):
            pass

    def update_runs(self, index: int | None) -> None:
        self.runs = []
        runs_table = self.query_one("#airflow-runs", DataTable)
        self._rendering_runs = True
        try:
            runs_table.clear()
        finally:
            self._rendering_runs = False
        if index is None or index >= len(self.filtered_dags) or self.selected_context is None:
            self._requested_runs_dag = None
            return
        dag = self.filtered_dags[index]
        self._requested_runs_dag = dag.dag_id
        self.query_one("#airflow-detail", RichLog).clear()
        self.query_one("#airflow-detail", RichLog).write(f"Loading runs for {dag.dag_id}...")
        self.run_worker(
            self._load_runs,
            name="airflow-runs",
            group="airflow-runs",
            exclusive=True,
            thread=True,
            exit_on_error=False,
        )

    def render_runs(self) -> None:
        table = self.query_one("#airflow-runs", DataTable)
        self._rendering_runs = True
        try:
            table.clear()
            for index, run in enumerate(self.runs):
                table.add_row(
                    run.dag_run_id,
                    run.state,
                    run.run_type,
                    format_duration(run.start_date, run.end_date),
                    format_dt(run.start_date),
                    key=str(index),
                )
            if table.row_count:
                table.move_cursor(row=0, scroll=False)
        finally:
            self._rendering_runs = False
        self.render_pools()

    def render_pools(self) -> None:
        detail = self.query_one("#airflow-detail", RichLog)
        detail.clear()
        detail.write("[bold]Pools[/bold]")
        if self.pools_error:
            detail.write(f"[yellow]Pools unavailable: {self.pools_error}[/yellow]")
        elif not self.pools:
            detail.write("No pools.")
        for pool in self.pools:
            detail.write(
                f"{pool.name}: {pool.occupied_slots}/{pool.slots} occupied, "
                f"running={pool.running_slots}, queued={pool.queued_slots}, open={pool.open_slots}"
            )
        detail.write("")
        detail.write("Select a run and press l to load task states.")

    def render_tasks(self, instances: list[AirflowTaskInstanceView]) -> None:
        detail = self.query_one("#airflow-detail", RichLog)
        detail.clear()
        run = self.selected_run()
        detail.write(f"[bold]Task instances[/bold] {run.dag_run_id if run else ''}")
        if not instances:
            detail.write("No task instances.")
            return
        for instance in instances:
            state = instance.state or "-"
            detail.write(
                f"{instance.task_id}: {state} "
                f"(try {instance.try_number}, {format_duration(instance.start_date, instance.end_date)})"
            )

    def action_load_tasks(self) -> None:
        dag = self.selected_dag()
        run = self.selected_run()
        if dag is None or run is None or self.selected_context is None:
            detail = self.query_one("#airflow-detail", RichLog)
            detail.clear()
            detail.write("Select a DAG and a run first.")
            return
        self._requested_tasks_key = (dag.dag_id, run.dag_run_id)
        detail = self.query_one("#airflow-detail", RichLog)
        detail.clear()
        detail.write(f"Loading task instances for {run.dag_run_id}...")
        self.run_worker(
            self._load_tasks,
            name="airflow-tasks",
            group="airflow-tasks",
            exclusive=True,
            thread=True,
            exit_on_error=False,
        )

    def action_trigger_dag(self) -> None:
        dag = self.selected_dag()
        if dag is None:
            self.show_error(AwsCliError("Select a DAG to trigger."))
            return
        self.push_screen(AirflowTriggerScreen(dag.dag_id), self.trigger_dag_from_form)

    def trigger_dag_from_form(self, values: dict[str, str] | None) -> None:
        if not values:
            return
        try:
            ctx = self.selected_context or self.primary_context()
            if ctx is None or not self.environment:
                raise AwsCliError("No AWS context or MWAA environment available.")
            dag_id = values.get("dag_id", "").strip()
            if not dag_id:
                raise AwsCliError("DAG id is required.")
            conf_text = values.get("conf", "").strip()
            conf = None
            if conf_text:
                import json as _json

                try:
                    conf = _json.loads(conf_text)
                except ValueError as exc:
                    raise AwsCliError(f"conf must be valid JSON: {exc}") from exc
                if not isinstance(conf, dict):
                    raise AwsCliError("conf must be a JSON object")
            run = trigger_airflow_dag(ctx, self.environment, dag_id, conf=conf)
            self.query_one("#status", Static).update(
                f"Triggered {dag_id}: {run.dag_run_id} ({run.state}) on {ctx.profile}/{ctx.region}."
            )
            self.action_refresh()
        except Exception as exc:
            self.show_error(exc)

    def selected_dag(self) -> AirflowDagView | None:
        table = self.query_one("#airflow-dags", DataTable)
        if not self.filtered_dags or table.cursor_row >= len(self.filtered_dags):
            return None
        return self.filtered_dags[table.cursor_row]

    def selected_run(self) -> AirflowDagRunView | None:
        table = self.query_one("#airflow-runs", DataTable)
        if not self.runs or table.cursor_row >= len(self.runs):
            return None
        return self.runs[table.cursor_row]


class AirflowTriggerScreen(ModalScreen[dict[str, str] | None]):
    def __init__(self, dag_id: str = "") -> None:
        super().__init__()
        self.dag_id = dag_id

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog"):
            yield Label("Trigger Airflow DAG")
            yield Input(value=self.dag_id, placeholder="DAG id", id="dag-id")
            yield Input(placeholder="Conf JSON (optional), e.g. {\"run_date\": \"2026-01-01\"}", id="conf")
            with Horizontal():
                yield Button("Trigger", id="trigger", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_mount(self) -> None:
        self.query_one("#dag-id", Input).focus()

    @on(Button.Pressed, "#trigger")
    def trigger(self) -> None:
        self.dismiss(
            {
                "dag_id": self.query_one("#dag-id", Input).value,
                "conf": self.query_one("#conf", Input).value,
            }
        )

    @on(Button.Pressed, "#cancel")
    def cancel(self) -> None:
        self.dismiss(None)


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
        table.add_row("ecs", "ECS clusters, services, running tasks, CloudWatch logs", key="ecs")
        table.add_row("airflow", "MWAA DAGs, recent runs, task states, pools, trigger", key="airflow")
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


class ProfileSelectScreen(ModalScreen[str | None]):
    BINDINGS = [
        Binding("up", "previous_profile", "Previous", priority=True),
        Binding("k", "previous_profile", "Previous", priority=True, show=False),
        Binding("down", "next_profile", "Next", priority=True),
        Binding("j", "next_profile", "Next", priority=True, show=False),
        Binding("enter", "select_profile", "Select", priority=True),
        Binding("escape", "cancel", "Cancel", priority=True),
    ]

    def __init__(self, profiles: list[str], current_profile: str | None) -> None:
        super().__init__()
        self.profiles = profiles
        self.current_profile = current_profile
        self.selected_index = 0
        self.visible_start = 0

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog"):
            yield Label(f"Choose AWS profile. Current: {self.current_profile or 'default/all profiles'}")
            yield Static("", id="profiles")

    def on_mount(self) -> None:
        for index, profile in enumerate(self.profiles):
            if profile == self.current_profile:
                self.selected_index = index
                break
        self.render_profiles()

    def on_key(self, event: events.Key) -> None:
        if event.key in {"down", "j"}:
            event.stop()
            self.action_next_profile()
        elif event.key in {"up", "k"}:
            event.stop()
            self.action_previous_profile()
        elif event.key == "enter":
            event.stop()
            self.action_select_profile()
        elif event.key == "escape":
            event.stop()
            self.action_cancel()

    def action_previous_profile(self) -> None:
        if self.profiles:
            self.selected_index = max(0, self.selected_index - 1)
            self.render_profiles()

    def action_next_profile(self) -> None:
        if self.profiles:
            self.selected_index = min(len(self.profiles) - 1, self.selected_index + 1)
            self.render_profiles()

    def action_select_profile(self) -> None:
        if not self.profiles:
            self.dismiss(None)
            return
        self.dismiss(self.profiles[self.selected_index])

    def action_cancel(self) -> None:
        self.dismiss(None)

    def render_profiles(self) -> None:
        self._keep_selection_visible()
        visible_end = min(len(self.profiles), self.visible_start + PROFILE_PICKER_VISIBLE_ROWS)
        lines = []
        for index in range(self.visible_start, visible_end):
            profile = self.profiles[index]
            cursor = ">" if index == self.selected_index else " "
            marker = " (current)" if profile == self.current_profile else ""
            lines.append(f"{cursor} {profile}{marker}")
        self.query_one("#profiles", Static).update("\n".join(lines))

    def _keep_selection_visible(self) -> None:
        if not self.profiles:
            self.visible_start = 0
            return
        max_start = max(0, len(self.profiles) - PROFILE_PICKER_VISIBLE_ROWS)
        if self.selected_index < self.visible_start:
            self.visible_start = self.selected_index
        elif self.selected_index >= self.visible_start + PROFILE_PICKER_VISIBLE_ROWS:
            self.visible_start = self.selected_index - PROFILE_PICKER_VISIBLE_ROWS + 1
        self.visible_start = max(0, min(self.visible_start, max_start))


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


def _fuzzy_subsequence_score(query: str, target: str) -> int | None:
    """Return a match score if every char of query appears in target in order.

    Lower is better. Contiguous matches and matches near the start score better,
    so "ame" ranks avm_end_to_end above a scattered match. Returns None on no match.
    """
    if not query:
        return 0
    score = 0
    last_index = -1
    start = 0
    for char in query:
        found = target.find(char, start)
        if found == -1:
            return None
        # Penalize gaps between matched characters and distance from the start.
        if last_index != -1:
            score += found - last_index - 1
        else:
            score += found
        last_index = found
        start = found + 1
    return score


def _fuzzy_filter_dags(dags: list[AirflowDagView], query: str) -> list[AirflowDagView]:
    """Filter and rank DAGs by a case-insensitive fuzzy match on dag_id."""
    query = query.strip().lower()
    if not query:
        return list(dags)
    scored: list[tuple[int, str, AirflowDagView]] = []
    for dag in dags:
        score = _fuzzy_subsequence_score(query, dag.dag_id.lower())
        if score is not None:
            scored.append((score, dag.dag_id, dag))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [dag for _, _, dag in scored]

