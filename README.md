# SageMaker Ops CLI

`smops` 是一个面向 SageMaker Processing Job 和 SageMaker Pipeline 的命令行工具：

- 提交 SageMaker Processing Job
- 启动 SageMaker Pipeline execution
- 用 TUI 查看正在运行的 Processing Jobs
- 用 TUI 查看正在运行的 Pipeline executions、steps 状态和失败 step 的 CloudWatch 日志尾部
- 支持单个、多个或所有 AWS profiles

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

如果要读取 YAML 配置：

```bash
pip install -e '.[yaml]'
```

## 提交 Processing Job

配置文件直接使用 boto3 `create_processing_job` 的参数结构。

```bash
smops processing submit \
  --profile dev \
  --region us-east-1 \
  --config examples/processing-job.json
```

只检查请求内容，不提交：

```bash
smops processing submit --config examples/processing-job.json --dry-run
```

## 启动 Pipeline

```bash
smops pipeline start \
  --profile dev \
  --region us-east-1 \
  --name my-pipeline \
  --display-name manual-run-001 \
  --parameter InputDate=2026-06-30 \
  --parameter Mode=prod
```

## TUI 查看 Processing Jobs

```bash
smops tui processing --profile dev --region us-east-1
```

多个 profile：

```bash
smops tui processing --profile dev --profile prod --region us-east-1
```

所有 profile：

```bash
smops tui processing --all-profiles
```

快捷键：

- `↑/↓` 或 `←/→` 切换 job
- `r` 刷新
- `q` 退出

## TUI 查看 Pipelines

```bash
smops tui pipelines --profile dev --region us-east-1
```

只看某个 pipeline：

```bash
smops tui pipelines --profile dev --region us-east-1 --name my-pipeline
```

快捷键：

- `←/→` 在 executions 和 steps 面板之间切换
- `↑/↓` 移动当前面板选中行
- `l` 加载选中失败 step 的 CloudWatch 日志尾部
- `r` 刷新
- `q` 退出

目前自动支持这些 step 的日志定位：

- ProcessingJob: `/aws/sagemaker/ProcessingJobs`
- TrainingJob: `/aws/sagemaker/TrainingJobs`
- TransformJob: `/aws/sagemaker/TransformJobs`

## 非交互式查看

```bash
smops processing list --profile dev --region us-east-1
smops pipeline list --profile dev --region us-east-1
smops pipeline steps --profile dev --region us-east-1 --execution-arn arn:aws:sagemaker:...
```

## AWS 权限

运行账号需要至少具备这些权限：

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

仓库里提供了一套 mock AWS 配置，方便本地演示 profile 切换和 CLI 参数解析，不会写入真实 `~/.aws`：

```bash
export AWS_CONFIG_FILE=examples/aws/config
export AWS_SHARED_CREDENTIALS_FILE=examples/aws/credentials
export AWS_PROFILE=mock-dev
export AWS_DEFAULT_REGION=us-east-1
```

也可以直接加载样例环境变量：

```bash
set -a
source examples/aws/mock.env
set +a
```

然后运行：

```bash
smops processing submit --config examples/processing-job.json --dry-run
smops processing list --profile mock-dev
smops tui processing --profile mock-dev
```

注意：这套 credentials 是 dummy 值，只适合 dry-run、mock、本地端点或配合 botocore Stubber/moto 使用；直接访问真实 AWS 会认证失败。

## E2E 测试

测试使用 `moto` 模拟 AWS SageMaker 和 CloudWatch Logs，不会访问真实 AWS：

```bash
pip install -e '.[dev]'
pytest
```

覆盖范围包括：

- Processing Job 提交和 running job 列表
- Pipeline execution 启动和 running execution 列表
- Pipeline steps 状态展示
- 失败 step 的 CloudWatch Logs tail
- Processing Job TUI 的上下左右键导航
- Pipeline TUI 的 executions、steps 和失败日志加载
- 多 AWS profile 解析

Moto 目前还没有实现 `list_pipeline_execution_steps`，测试里对这一个 paginator 做了内存 fake，其余 SageMaker/Logs 调用都在 moto 环境中执行。
