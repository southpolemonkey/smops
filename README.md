# SageMaker Ops CLI

`smops` is a command-line tool for operating Amazon SageMaker Processing Jobs and SageMaker Pipelines.

It can:

- Submit SageMaker Processing Jobs
- Start SageMaker Pipeline executions
- Show running Processing Jobs in an interactive TUI
- Show running and recently completed Pipeline executions in an interactive TUI
- Inspect Pipeline step status and failed step CloudWatch logs
- Monitor Amazon MWAA (Apache Airflow) DAG status, runs, task states, and pools, and trigger DAGs
- Work with one, many, or all configured AWS profiles

## Installation

Install from PyPI:

```bash
pip install sagemaker-ops-cli
```

The installed command is:

```bash
smops --help
```

Install from GitHub:

```bash
pip install git+https://github.com/southpolemonkey/smops.git
```

Install from a local wheel:

```bash
pip install dist/sagemaker_ops_cli-0.2.0-py3-none-any.whl
```

Install with Homebrew:

```bash
brew tap southpolemonkey/smops https://github.com/southpolemonkey/smops
brew install sagemaker-ops-cli
```

If the formula is later moved into a dedicated `southpolemonkey/homebrew-smops` tap repository, users can use the shorter command:

```bash
brew tap southpolemonkey/smops
brew install sagemaker-ops-cli
```

For local development:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

To enable YAML config files:

```bash
pip install -e '.[yaml]'
```

## Defaults

Set a default AWS region once so you do not need to pass `--region` on every command:

```bash
smops config set-region ap-southeast-2
smops config get-region
```

The config file is stored at `~/.config/smops/config.json` by default. You can inspect it with:

```bash
smops config show
smops config path
```

Region resolution order is:

1. `--region`
2. `SMOPS_DEFAULT_REGION`
3. `smops config set-region ...`
4. Region configured on the selected AWS profile

## Build The Python Package

```bash
pip install -e '.[dev]'
python -m build
```

Build artifacts are written to `dist/`:

- `sagemaker_ops_cli-0.2.0-py3-none-any.whl`
- `sagemaker_ops_cli-0.2.0.tar.gz`

## Submit A Processing Job

The config file uses the same parameter structure as boto3 `create_processing_job`.

```bash
smops processing submit \
  --profile dev \
  --region us-east-1 \
  --config examples/processing-job.json
```

Validate the request without submitting it:

```bash
smops processing submit --config examples/processing-job.json --dry-run
```

## Start A Pipeline Execution

```bash
smops pipeline start \
  --profile dev \
  --region us-east-1 \
  --name my-pipeline \
  --display-name manual-run-001 \
  --parameter InputDate=2026-06-30 \
  --parameter Mode=prod
```

## ECS Task Inspection

`smops` can also inspect ECS clusters, services, tasks, and awslogs-backed task logs:

```bash
smops ecs clusters --profile dev --region us-east-1
smops ecs services --profile dev --region us-east-1 --cluster my-cluster
smops ecs tasks --profile dev --region us-east-1 --cluster my-cluster --service my-service
smops ecs task --profile dev --region us-east-1 --cluster my-cluster --task arn:aws:ecs:...
smops ecs logs --profile dev --region us-east-1 --cluster my-cluster --task arn:aws:ecs:... --container app
```

ECS log discovery supports containers using the `awslogs` log driver. If a task has multiple awslogs containers, pass `--container`.

## ECS TUI

```bash
smops tui ecs --profile dev --region us-east-1
```

The ECS TUI shows clusters, services, running tasks, and CloudWatch logs.

Keyboard shortcuts:

- `Left` / `Right`: switch focus between clusters, services, and tasks
- `Up` / `Down`: move within the focused table
- `l`: load CloudWatch logs for the selected running task
- `p` / `P`: choose an AWS profile
- `r`: refresh
- `q`: quit

Log discovery uses the selected task's task definition and supports containers using the `awslogs` log driver.

## Airflow (Amazon MWAA)

`smops` can monitor and trigger DAGs in an Amazon MWAA environment. It reads the
Airflow REST API on the environment's private web server: it obtains a short-lived
web login token via `mwaa:CreateWebLoginToken`, exchanges it for a session cookie,
then calls the `/api/v1/...` endpoints. Your credentials therefore need
`airflow:CreateWebLoginToken` (or the equivalent MWAA permission) and network access
to the web server host.

```bash
smops airflow environments --profile dev --region ap-southeast-2
smops airflow dags --profile dev --env my-environment --pattern avm
smops airflow runs --profile dev --env my-environment --dag my_dag
smops airflow tasks --profile dev --env my-environment --dag my_dag --run manual__2026-01-01T00:00:00+00:00
smops airflow pools --profile dev --env my-environment
smops airflow trigger --profile dev --env my-environment --dag my_dag --conf '{"run_date": "2026-01-01"}'
```

`airflow pools` shows pool slot usage (Airflow's concurrency "locks"). `airflow trigger`
prompts for confirmation before starting a run; pass `--yes` (required with `--json`)
to skip the prompt.

To avoid repeating `--env`, set a default environment:

```bash
smops config set-mwaa-env my-environment
smops config get-mwaa-env
```

The default can also be set with the `SMOPS_MWAA_ENVIRONMENT` environment variable,
which overrides the config file.

## Airflow TUI

```bash
smops tui airflow --profile dev --env my-environment
```

The Airflow TUI shows DAGs, the recent runs for the selected DAG, and pool usage,
and it can load per-task states and trigger a DAG.

Keyboard shortcuts:

- `Left` / `Right`: switch focus between the DAGs and runs tables
- `Up` / `Down`: move within the focused table
- `/`: fuzzy-search DAGs by name (type to filter, `Enter` to jump to the top match, `Esc` to clear)
- `l`: load task-instance states for the selected run
- `t`: trigger the selected DAG
- `p` / `P`: choose an AWS profile
- `r`: refresh
- `q`: quit

## Interactive TUI

Open the TUI selector and choose between Pipelines and Processing Jobs:

```bash
smops tui --profile dev
```

Inside the TUI:

- `p` / `P`: switch to the next AWS profile from your local AWS config
- `s`: start a pipeline or submit a processing job from the current TUI
- `r`: refresh
- `q`: quit

For pipeline starts, enter the pipeline name, optional display name, and optional comma-separated parameters such as `InputDate=2026-07-01,Mode=test`. For processing job submits, enter the path to a JSON/YAML config file using the same structure as boto3 `create_processing_job`.

## Processing Jobs TUI

```bash
smops tui processing --profile dev --region us-east-1
```

Multiple profiles:

```bash
smops tui processing --profile dev --profile prod --region us-east-1
```

All profiles:

```bash
smops tui processing --all-profiles
```

Keyboard shortcuts:

- `Up` / `Down` or `Left` / `Right`: switch jobs
- `p` / `P`: switch to the next AWS profile
- `s`: submit a Processing Job from a JSON/YAML config file
- `r`: refresh
- `q`: quit

## Pipelines TUI

```bash
smops tui pipelines --profile dev --region us-east-1
```

Filter to one pipeline:

```bash
smops tui pipelines --profile dev --region us-east-1 --name my-pipeline
```

By default, the TUI shows running executions plus executions completed within the last 3 hours, so you can inspect recent success and failure results. Use `--hours` to adjust the time window:

```bash
smops tui pipelines --profile dev --region us-east-1 --name my-pipeline --hours 6
```

Keyboard shortcuts:

- `Left` / `Right`: switch focus between the executions and steps panels
- `Up` / `Down`: move within the focused panel
- `p` / `P`: switch to the next AWS profile
- `s`: start a Pipeline execution
- `l`: load the CloudWatch log tail for the selected failed step
- `r`: refresh
- `q`: quit

Log discovery is currently supported for these step job types:

- ProcessingJob: `/aws/sagemaker/ProcessingJobs`
- TrainingJob: `/aws/sagemaker/TrainingJobs`
- TransformJob: `/aws/sagemaker/TransformJobs`

## Non-Interactive Commands

```bash
smops processing list --profile dev --region us-east-1
smops processing wait --profile dev --region us-east-1 --name my-processing-job
smops pipeline list --profile dev --region us-east-1
smops pipeline list --profile dev --region us-east-1 --name my-pipeline --hours 6
smops pipeline steps --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:...
smops pipeline wait --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:...
smops pipeline inspect --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:...
smops pipeline diagnose --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:...
smops airflow environments --profile dev --region ap-southeast-2
smops airflow runs --profile dev --env my-environment --dag my_dag
smops airflow pools --profile dev --env my-environment
```

Most non-interactive commands support `--json` for agents and automation:

```bash
smops processing list --profile dev --region us-east-1 --json
smops processing wait --profile dev --region us-east-1 --name my-processing-job --json
smops pipeline start --profile dev --region us-east-1 --name my-pipeline --json
smops pipeline list --profile dev --region us-east-1 --json
smops pipeline steps --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:... --json
smops pipeline wait --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:... --json
smops pipeline inspect --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:... --json
smops pipeline diagnose --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:... --json
smops airflow runs --profile dev --env my-environment --dag my_dag --json
smops airflow trigger --profile dev --env my-environment --dag my_dag --yes --json
```

JSON responses use a stable envelope. Successful commands return `status: "ok"`; errors return `status: "error"` and a user-facing `error` message. List commands return `items`, `count`, and `next_token`.

`pipeline inspect` returns execution details, all steps, and failed steps. `pipeline diagnose` extends that with the first failed step, inferred SageMaker job type/name, CloudWatch log group and stream prefix, log tail, and suggested next actions.

`processing list` reads 20 running jobs per page by default. If the output includes `Next token`, pass it to fetch the next page:

```bash
smops processing list --profile dev --region us-east-1 --max-results 20
smops processing list --profile dev --region us-east-1 --max-results 20 --next-token '<token>'
```

When `pipeline list` is used without `--name`, it scans 10 pipelines per page by default. This avoids long hangs in AWS accounts with many pipelines. If the output includes `Next token`, pass it to continue scanning:

```bash
smops pipeline list --profile dev --region us-east-1 --pipeline-page-size 10
smops pipeline list --profile dev --region us-east-1 --pipeline-page-size 10 --next-token '<token>'
```

## AWS Permissions

The AWS identity used by `smops` needs at least these permissions:

- `sagemaker:CreateProcessingJob`
- `sagemaker:StartPipelineExecution`
- `sagemaker:ListProcessingJobs`
- `sagemaker:DescribeProcessingJob`
- `sagemaker:ListPipelines`
- `sagemaker:ListPipelineExecutions`
- `sagemaker:DescribePipelineExecution`
- `sagemaker:ListPipelineExecutionSteps`
- `logs:DescribeLogStreams`
- `logs:GetLogEvents`

## Mock AWS Profile

This repository includes mock AWS config files for local demos of profile switching and CLI argument parsing. They do not write to your real `~/.aws` files:

```bash
export AWS_CONFIG_FILE=examples/aws/config
export AWS_SHARED_CREDENTIALS_FILE=examples/aws/credentials
export AWS_PROFILE=mock-dev
export AWS_DEFAULT_REGION=us-east-1
```

You can also load the sample environment file directly:

```bash
set -a
source examples/aws/mock.env
set +a
```

Then run:

```bash
smops processing submit --config examples/processing-job.json --dry-run
smops processing list --profile mock-dev
smops tui processing --profile mock-dev
```

The bundled credentials are dummy values. They are only intended for dry runs, mock environments, local endpoints, or tests that use botocore Stubber/moto. They will not authenticate against real AWS.

## E2E Tests

The tests use `moto` to simulate AWS SageMaker and CloudWatch Logs. They do not call real AWS services:

```bash
pip install -e '.[dev]'
pytest
```

Coverage includes:

- Processing Job submission and paginated running job lists
- Pipeline execution start and active/recent execution lists
- Pipeline step status display
- Failed step CloudWatch Logs tailing
- Processing Job TUI keyboard navigation with up, down, left, and right
- Pipeline TUI execution, step, and failed log loading
- Multiple AWS profile resolution

Moto does not currently implement `list_pipeline_execution_steps`, so that paginator is faked in memory in the tests. The other SageMaker and CloudWatch Logs calls run inside the moto environment.
