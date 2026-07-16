# smops product demo
#
# Usage:
#   export SMOPS_DEMO_PROFILE=your-aws-profile
#   export SMOPS_DEMO_REGION=ap-southeast-2
#   export SMOPS_DEMO_PIPELINE=your-pipeline-name
#   vhs demo/smops-demo.tape
#
# The recording uses a real AWS account/profile. Pick a profile that can read
# SageMaker Pipelines, Processing Jobs, and CloudWatch Logs.

Output demo/smops-demo.gif

Require bash

Set Shell "bash"
Set FontSize 22
Set Width 1280
Set Height 900
Set Padding 24
Set Margin 12
Set MarginFill "#0f172a"
Set BorderRadius 10
Set WindowBar Colorful
Set TypingSpeed 35ms
Set Framerate 30
Set Theme "Dracula"

Type "clear"
Enter
Sleep 400ms

Type "export SMOPS_DEMO_PROFILE=${SMOPS_DEMO_PROFILE:-mock-dev}"
Enter
Type "export SMOPS_DEMO_REGION=${SMOPS_DEMO_REGION:-ap-southeast-2}"
Enter
Type "export SMOPS_DEMO_PIPELINE=${SMOPS_DEMO_PIPELINE:-demo-pipeline}"
Enter
Sleep 300ms

Type "cd /Users/chenxuan.rong/projects/smops"
Enter
Sleep 700ms

Type "source .venv/bin/activate"
Enter
Sleep 700ms

Type "smops --help"
Enter
Sleep 2s

Type "clear"
Enter
Sleep 300ms
Type "smops pipeline list --profile $SMOPS_DEMO_PROFILE --region $SMOPS_DEMO_REGION --name $SMOPS_DEMO_PIPELINE --hours 3"
Enter
Sleep 4s

Type "clear"
Enter
Sleep 300ms
Type "smops tui pipelines --profile $SMOPS_DEMO_PROFILE --region $SMOPS_DEMO_REGION --name $SMOPS_DEMO_PIPELINE --hours 3 --refresh 60"
Enter
Sleep 3s

# Move into steps, then load the selected step log tail.
Right
Sleep 700ms
Down
Sleep 500ms
Type "l"
Sleep 3s

# Show the profile picker. This demonstrates the current-profile marker and
# scrollable picker behavior for larger AWS config files.
Type "p"
Sleep 1s
Down
Sleep 300ms
Down
Sleep 300ms
Up
Sleep 300ms
Escape
Sleep 700ms

Type "q"
Sleep 700ms

Type "clear"
Enter
Sleep 300ms
Type "smops tui processing --profile $SMOPS_DEMO_PROFILE --region $SMOPS_DEMO_REGION --refresh 60"
Enter
Sleep 3s

# Processing TUI: navigate jobs and load CloudWatch ProcessingJobs logs.
Down
Sleep 500ms
Type "l"
Sleep 3s
Type "p"
Sleep 1s
Escape
Sleep 500ms
Type "q"
Sleep 700ms

Type "clear"
Enter
Sleep 300ms
Type "echo 'smops: SageMaker operations from your terminal'"
Enter
Sleep 1s
