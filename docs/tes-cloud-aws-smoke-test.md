# TES Cloud Smoke Test (AWS Batch + Fargate)

This smoke test validates that **OmniBioAI TES tool runtime** can execute on **AWS Batch (Fargate)** and write deterministic outputs to **S3** via `RESULT_URI`.

It is intentionally minimal: **one tool (`echo_test`) → one run → one results.json**.

---

## 0) Preconditions

- AWS CLI configured and working:

```bash
aws sts get-caller-identity
aws configure get region
````

* Region used in this setup: `us-east-1`
* Docker image exists on Docker Hub and is multi-arch:

```bash
docker buildx imagetools inspect man4ish/omnibioai-tool-runtime:latest
```

---

## 1) S3 bucket for results

Set your values:

```bash
export AWS_REGION="us-east-1"
export ACCOUNT_ID="068155050939"
export BUCKET="omnibioai-tes-results-${ACCOUNT_ID}"
```

Create bucket (if not already):

```bash
aws s3 mb "s3://${BUCKET}" --region "${AWS_REGION}" || true
aws s3 ls | grep "${BUCKET}"
```

---

## 2) AWS Batch: Compute Environment + Job Queue

Assumes default VPC in region.

### 2.1 Create AWS Batch service role (one-time)

```bash
aws iam create-role \
  --role-name AWSBatchServiceRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": { "Service": "batch.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }]
  }' 2>/dev/null || true

aws iam attach-role-policy \
  --role-name AWSBatchServiceRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole
```

### 2.2 Pick two subnets + default security group

```bash
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)

SUBNET1=$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC_ID --query 'Subnets[0].SubnetId' --output text)
SUBNET2=$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC_ID --query 'Subnets[1].SubnetId' --output text)

SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$VPC_ID Name=group-name,Values=default --query 'SecurityGroups[0].GroupId' --output text)

echo "VPC=$VPC_ID"
echo "SUBNETS=$SUBNET1 $SUBNET2"
echo "SG=$SG"
```

### 2.3 Create compute environment (Fargate)

```bash
aws batch create-compute-environment \
  --compute-environment-name omnibioai-ce-fargate \
  --type MANAGED \
  --state ENABLED \
  --compute-resources "type=FARGATE,maxvCpus=16,subnets=[$SUBNET1,$SUBNET2],securityGroupIds=[$SG]" \
  --service-role "arn:aws:iam::${ACCOUNT_ID}:role/AWSBatchServiceRole"
```

Wait until VALID:

```bash
aws batch describe-compute-environments \
  --compute-environments omnibioai-ce-fargate \
  --query 'computeEnvironments[0].{status:status,state:state,statusReason:statusReason}' \
  --output table
```

### 2.4 Create job queue

```bash
aws batch create-job-queue \
  --job-queue-name omnibioai-jq \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order order=1,computeEnvironment=omnibioai-ce-fargate
```

Verify:

```bash
aws batch describe-job-queues \
  --job-queues omnibioai-jq \
  --query 'jobQueues[0].{name:jobQueueName,status:status,state:state,statusReason:statusReason}' \
  --output table
```

---

## 3) IAM roles for Fargate task

### 3.1 ECS task execution role (pull image + CloudWatch logs)

```bash
cat > /tmp/ecs-task-exec-trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON

aws iam create-role \
  --role-name ecsTaskExecutionRole \
  --assume-role-policy-document file:///tmp/ecs-task-exec-trust.json 2>/dev/null || true

aws iam attach-role-policy \
  --role-name ecsTaskExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

### 3.2 Job role (allow tool runtime to write results to S3)

```bash
cat > /tmp/omnibioaiBatchJobRole-trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON

aws iam create-role \
  --role-name omnibioaiBatchJobRole \
  --assume-role-policy-document file:///tmp/omnibioaiBatchJobRole-trust.json 2>/dev/null || true

cat > /tmp/omnibioaiBatchJobRole-s3.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject","s3:AbortMultipartUpload","s3:ListBucketMultipartUploads","s3:ListMultipartUploadParts"],
    "Resource": [
      "arn:aws:s3:::${BUCKET}",
      "arn:aws:s3:::${BUCKET}/*"
    ]
  }]
}
JSON

aws iam put-role-policy \
  --role-name omnibioaiBatchJobRole \
  --policy-name omnibioaiBatchJobRoleS3Put \
  --policy-document file:///tmp/omnibioaiBatchJobRole-s3.json
```

If submitting as an IAM user, ensure PassRole:

```bash
cat > /tmp/allow-passrole.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "iam:PassRole",
    "Resource": [
      "arn:aws:iam::${ACCOUNT_ID}:role/ecsTaskExecutionRole",
      "arn:aws:iam::${ACCOUNT_ID}:role/omnibioaiBatchJobRole"
    ]
  }]
}
JSON

# Replace <IAM_USER> with your user name (e.g. tes-dev)
aws iam put-user-policy \
  --user-name <IAM_USER> \
  --policy-name AllowPassBatchRoles \
  --policy-document file:///tmp/allow-passrole.json
```

---

## 4) CloudWatch log group (recommended)

```bash
aws logs create-log-group --log-group-name /aws/batch/job 2>/dev/null || true
aws logs put-retention-policy --log-group-name /aws/batch/job --retention-in-days 7
```

---

## 5) Register job definition (echo_test)

```bash
cat > /tmp/omnibioai-echo-test-fargate.json <<JSON
{
  "jobDefinitionName": "omnibioai-echo-test-fargate",
  "type": "container",
  "platformCapabilities": ["FARGATE"],
  "containerProperties": {
    "image": "man4ish/omnibioai-tool-runtime:latest",
    "command": ["python", "-m", "tools.echo_test.run"],
    "executionRoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/ecsTaskExecutionRole",
    "jobRoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/omnibioaiBatchJobRole",
    "resourceRequirements": [
      {"type": "VCPU", "value": "0.25"},
      {"type": "MEMORY", "value": "512"}
    ],
    "networkConfiguration": { "assignPublicIp": "ENABLED" },
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/aws/batch/job",
        "awslogs-region": "${AWS_REGION}",
        "awslogs-stream-prefix": "omnibioai"
      }
    },
    "environment": [
      {"name": "TOOL_ID", "value": "echo_test"},
      {"name": "RUN_ID", "value": "aws-batch-smoke-default"},
      {"name": "INPUTS_JSON", "value": "{\"text\":\"hello from aws batch\"}"},
      {"name": "RESOURCES_JSON", "value": "{}"},
      {"name": "RESULT_URI", "value": "s3://${BUCKET}/tes-runs/aws-batch-smoke-default/tools/echo_test/results.json"}
    ]
  }
}
JSON

python -m json.tool /tmp/omnibioai-echo-test-fargate.json >/dev/null && echo "jobdef JSON OK"

aws batch register-job-definition --cli-input-json file:///tmp/omnibioai-echo-test-fargate.json
```

---

## 6) Submit job with per-run overrides (recommended pattern)

This mirrors TES behavior: job definition is static, but `RUN_ID` and `RESULT_URI` are override-safe.

```bash
RUN_ID="aws-batch-smoke-$(date +%Y%m%d-%H%M%S)"
RESULT_URI="s3://${BUCKET}/tes-runs/${RUN_ID}/tools/echo_test/results.json"

JOB_ID=$(aws batch submit-job \
  --job-name "omnibioai-echo-${RUN_ID}" \
  --job-queue omnibioai-jq \
  --job-definition omnibioai-echo-test-fargate \
  --container-overrides "environment=[{name=RUN_ID,value=${RUN_ID}},{name=RESULT_URI,value=${RESULT_URI}}]" \
  --query jobId --output text)

echo "JOB_ID=$JOB_ID"
echo "RUN_ID=$RUN_ID"
echo "RESULT_URI=$RESULT_URI"
```

Poll until finished:

```bash
while true; do
  STATUS=$(aws batch describe-jobs --jobs "$JOB_ID" --query 'jobs[0].status' --output text)
  echo "STATUS=$STATUS"
  if [ "$STATUS" = "SUCCEEDED" ]; then break; fi
  if [ "$STATUS" = "FAILED" ]; then
    aws batch describe-jobs --jobs "$JOB_ID" --query 'jobs[0].statusReason' --output text
    exit 2
  fi
  sleep 5
done
```

Fetch results:

```bash
aws s3 cp "$RESULT_URI" - | python -m json.tool
```

Expected output (example):

```json
{
  "ok": true,
  "tool_id": "echo_test",
  "run_id": "aws-batch-smoke-...",
  "results": { "echo": "hello from aws batch" }
}
```

---

## 7) Debugging (logs)

Get Batch log stream:

```bash
LOG_STREAM=$(aws batch describe-jobs --jobs "$JOB_ID" --query 'jobs[0].container.logStreamName' --output text)
echo "LOG_STREAM=$LOG_STREAM"

aws logs get-log-events \
  --log-group-name /aws/batch/job \
  --log-stream-name "$LOG_STREAM" \
  --limit 200 \
  --query 'events[*].message' --output text
```

---

## Notes / Known Pitfalls

* `exec format error` in Fargate means you ran the wrong architecture image (must include `linux/amd64`).
* If job is stuck in PENDING/STARTING with CloudWatch errors, ensure:

  * log group exists: `/aws/batch/job`
  * `executionRoleArn` has `AmazonECSTaskExecutionRolePolicy`
  * `assignPublicIp=ENABLED` (or NAT connectivity)
* Always override `RUN_ID` + `RESULT_URI` at submit time to match TES semantics.
