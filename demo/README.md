# smops VHS Demo

Record the product demo with [VHS](https://github.com/charmbracelet/vhs):

```bash
export SMOPS_DEMO_PROFILE=your-aws-profile
export SMOPS_DEMO_REGION=ap-southeast-2
export SMOPS_DEMO_PIPELINE=your-pipeline-name
vhs demo/smops-demo.tape
```

The tape records:

- `smops --help`
- `smops pipeline list`
- Pipeline TUI loading, steps, logs, and profile picker
- Processing TUI split view with CloudWatch logs

It uses a real AWS account/profile, so choose a profile with SageMaker and CloudWatch Logs read permissions.
