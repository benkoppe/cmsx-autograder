# CMSX Autograder Plan

## Goal

This document is the living plan for a self-hostable CMSX autograding system where instructors can create assignment-specific autograders using simple Python grading scripts, while the platform handles CMSX webhook ingestion, job execution, reproducible runner environments, result storage, logs, job status, and future web UI integration. It is intended primarily as context for future contributors and fresh AI agents.

This plan is not completely strict. Prefer the choices here by default, but if implementation work reveals a clearly better approach, take the better approach and update this document to match reality.

The first practical execution backend should use a long-running worker container with access to a Docker runtime. The project should still be designed around an executor interface so rootless Docker, Podman, host executor daemons, remote executors, and Firecracker or other microVM backends can be added later without changing grader scripts or assignment semantics.

## Product Model

CMSX sends assignment submissions to an assignment-specific webhook URL. The control plane validates the request, stores the submission, creates a grading job, dispatches the job to an available worker, receives logs and status events while the job runs, stores the final result, and exposes all of that state for monitoring and a future web UI.

Publishing grades back to CMSX is not part of the initial core. Results should be stored internally from the beginning so CMSX grade publishing can be added later as a separate integration.

## High-Level Architecture

```text
CMSX
  |
  v
Control Plane / Big Brain
  - assignment webhook URLs
  - token validation
  - multipart submission parsing
  - submission storage
  - job queue
  - event and log store
  - result store
  - future web UI API
  |
  v
Worker
  - long-running deployable service
  - connects to control plane
  - authenticates with a provisioned private key
  - claims jobs
  - prepares input/grader/output workspaces
  - invokes executor backend
  - streams job events and logs
  - uploads final result and artifacts
  |
  v
Executor Backend
  - Docker socket executor first
  - in-worker executor for trusted/simple jobs
  - rootless Docker/Podman later
  - host executor daemon later
  - Firecracker or other microVM later
  |
  v
Runner Environment
  - Nix-built image or environment
  - Python grader SDK
  - assignment toolchains and libraries
  - no control-plane secrets
  |
  v
grade.py
  - instructor-authored Python grading script
  - reads submission files
  - optionally compiles or runs student code
  - writes structured result
```

## Core Principles

CMSX ingestion, grading execution, result storage, and CMSX grade publishing are separate concerns. The system should not couple webhook parsing to how jobs are executed or how grades are eventually published.

The grader contract should be independent of Docker, Podman, Firecracker, or any specific runtime. A `grade.py` script should see the same filesystem layout and SDK whether it runs in a Docker job container, an in-worker trusted mode, or a future microVM.

Nix should define and build reproducible environments. It should not be treated as the primary production isolation mechanism for dynamic student submissions.

Job logs and status events are core data, not a UI afterthought. The worker should stream structured lifecycle events and stdout/stderr while jobs run, and the control plane should store those events durably enough for later inspection and UI replay.

Trust boundaries should be explicit. A worker using the Docker socket is trusted infrastructure with powerful host access. Job containers should be the initial isolation boundary for grader and student code, and they must not receive the Docker socket or control-plane credentials.

## Implementation Languages

The core control plane and worker should be written in Rust unless a later implementation decision deliberately chooses otherwise. Rust fits the long-running infrastructure parts of the system: typed job/result/event models, worker orchestration, executor interfaces, Docker API integration, timeout handling, cancellation, cleanup, and future Firecracker or other VM lifecycle management.

Rust should not leak into the instructor-facing grading model. Instructors should write Python `grade.py` scripts using the grader SDK, regardless of whether the submitted student code is Python, C, Java, Node, or another language.

Python should be used for the grader authoring surface, the grader SDK, and optional grading helper libraries such as PDF parsing, structured file checks, subprocess helpers, and result construction.

Nix should be used for reproducible builds and environments: worker images, runner OCI images, dependency-pinned language/toolchain environments, development shells, and future Firecracker root filesystems or VM images.

TypeScript should be reserved for the future web UI and browser-facing code. The web UI should be built around the stable control plane APIs rather than driving the core architecture.

## CMSX Integration

Each assignment has a unique CMSX webhook URL, for example:

```text
https://autograder.example.com/cmsx/a/{assignment_slug}/submit
```

CMSX sends `multipart/form-data` containing assignment metadata, group metadata, an authentication token, file metadata, and uploaded file parts.

Expected fields include:

```text
auth_token
netids
assignment_id
assignment_name
num_files
problem_name_i
file_name_i
uploaded file part named by file_name_i
```

The receiver must validate the assignment slug and `auth_token`, parse the multipart body, store raw CMSX metadata for debugging, normalize submission metadata into internal models, store uploaded files, create a grading job, and return quickly. It should store CMSX's submitted `assignment_id` as request metadata, but should not require users to configure the CMSX assignment ID in this system or validate the submitted value against assignment configuration. The assignment-specific webhook URL plus assignment token are the binding between CMSX and the local assignment; requiring a separately entered CMSX assignment ID would add setup friction without materially improving the initial security model.

The initial receiver assumes `auth_token` appears before uploaded file parts, matching the order shown in the CMSX autograder guide. This avoids writing unauthenticated uploads to object storage. This assumption should be verified against real CMSX traffic; if CMSX does not follow this order in practice, the receiver should be changed to support arbitrary multipart ordering with a safe pre-auth spooling strategy.

Local assignment identity is `assignments.slug`. CMSX assignment IDs are external request metadata and should not be required for assignment setup unless a future CMSX API integration needs them for grade publishing or reconciliation.

CMSX documentation says only newest files are sent for an assignment submission. The system should not assume each webhook contains a complete project unless that behavior is verified. If full submission reconstruction is needed, it should be implemented as an explicit submission-state feature rather than assumed by the grader.

CMSX documentation also appears inconsistent about `netids` separators. The receiver should preserve the raw `netids` value and avoid destructive parsing until real request behavior is confirmed.

The supplied CMSX integration describes outbound submission delivery to an autograder URL. It does not define result submission back to CMSX. Internal result storage is therefore mandatory, and CMSX grade publishing should be a later integration.

## Control Plane

The control plane is the durable source of truth for assignments, submissions, jobs, results, workers, logs, and artifacts.

Responsibilities include:

- Assignment-specific webhook routes.
- Assignment token validation.
- CMSX multipart parsing.
- Submission metadata and file storage.
- Job creation and queueing.
- Worker authentication and heartbeats.
- Worker provisioning and key management.
- Event and log ingestion.
- Result and artifact storage.
- Job status APIs.
- Result inspection APIs.
- Future web UI APIs.
- Future CMSX grade publishing.

The control plane should avoid depending on executor-specific details. It should describe what a job needs, not how a worker must implement it.

## Worker

The worker is a long-running service deployed by the user or operator. The most practical initial deployment is a Docker container managed by Docker Compose.

Responsibilities include:

- Authenticate to the control plane using a provisioned private key.
- Send heartbeats with capacity and version information.
- Claim jobs from the control plane.
- Download submission files and grader bundles.
- Prepare per-job workspaces.
- Invoke the selected executor backend.
- Stream ordered job events and logs.
- Enforce timeout and cancellation from outside the job.
- Validate final result JSON.
- Upload final result and artifacts.
- Clean up containers, processes, and temporary files.

Workers should be provisioned by the control plane. Provisioning creates the internal worker ID, worker row, and an Ed25519 keypair; stores only the public key and fingerprint; and returns a worker config containing the control-plane URL plus a single-line base64-encoded private key. The worker does not need to know or configure its internal worker ID.

Worker requests should be signed with short-lived Ed25519 JWTs. The JWT key ID should be the public-key fingerprint derived from the provisioned private key, allowing the control plane to resolve the internal worker ID from `worker_keys`. Worker authentication should bind the JWT to method, path, and request body hash, and replay should be rejected using per-worker JWT IDs.

Worker credentials should be scoped by route behavior. They should allow the worker to claim assigned jobs, fetch required job inputs, upload events, upload results, and heartbeat. They should not allow assignment mutation, worker provisioning, key management, or unrestricted access to unrelated submissions.

Each worker process should run exactly one configured executor backend. The executor is local worker configuration rather than scheduler state stored by the control plane. In the initial system, connected workers are assumed to be operationally compatible with jobs from the same deployment; heterogeneous worker scheduling can be added later if it becomes necessary.

## Executor Backends

Execution should be modeled behind an interface. The rest of the system should interact with an executor using a stable job specification and receive a stream of execution events plus a final execution result.

Conceptually:

```text
Executor.run(job_spec, event_sink) -> execution_result
```

The executor is responsible for making the stable filesystem contract available, enforcing backend-specific isolation, capturing stdout/stderr, reporting lifecycle events, and returning enough information for the worker to validate and upload the final result.

### Docker Socket Executor

The initial main executor should use a worker container with access to the host Docker socket. The worker launches one short-lived job container per grading job.

Deployment shape:

```text
host machine
  Docker daemon
  worker container
    mounted Docker socket
    host-visible workspace mount
    launches per-job containers
  job container
    no Docker socket
    no control-plane secrets
    runs grade.py
```

The worker is trusted infrastructure in this mode. Mounting the Docker socket gives the worker root-equivalent control over the host through the Docker daemon. This must be documented clearly, and operators should be encouraged to run workers on dedicated hosts or VMs.

Job containers should be launched with strict defaults:

```text
network disabled by default
memory limit
CPU limit
PID limit
capabilities dropped
no-new-privileges
read-only root filesystem where practical
non-root user
input and grader mounts read-only
work and output mounts writable
tmpfs for temporary space
no Docker socket
no control-plane credentials
```

When a worker container uses the host Docker socket, bind mount paths passed to job containers are interpreted on the host, not inside the worker container. The worker therefore needs a host-visible workspace mounted at the same path inside the worker.

Example workspace:

```text
/srv/cmsx-worker/jobs/{job_id}/input
/srv/cmsx-worker/jobs/{job_id}/grader
/srv/cmsx-worker/jobs/{job_id}/work
/srv/cmsx-worker/jobs/{job_id}/output
```

### In-Worker Executor

The in-worker executor runs the grader directly inside the worker environment using the same job contract.

This mode is useful for local development, trusted graders, simple file inspection, PDF form extraction, and deployments where the operator accepts the risk of running the job inside the worker container.

This mode is not appropriate for arbitrary student code. A compromised job can affect the worker container and potentially interfere with future jobs or worker behavior.

### Future Executors

Future executor backends should preserve the same job contract and event model.

Potential backends include:

- Rootless Docker.
- Rootless Podman.
- A narrow host executor daemon.
- A remote executor service.
- Firecracker or another microVM backend.

Firecracker should remain a long-term design target for stronger isolation. Nix can be used to build future Firecracker root filesystems or VM images using the same conceptual runner environment definitions.

## Runner Environments

A runner environment contains the tools needed to execute `grade.py` and any student code or file-processing libraries required for an assignment.

Initial runner environments should include:

- `python`
- `python-pdf`
- `c-python`
- `node-python`
- `java-python`

For the Docker socket executor, runner environments should be packaged as Nix-built OCI images. Assignment configuration should reference pinned image digests where possible rather than floating tags.

The runner environment should not contain assignment auth tokens, worker private keys, CMSX API tokens, database credentials, or other control-plane secrets.

## Nix Usage

Nix should be used to build and pin the system's executable environments.

Use Nix for:

- Worker images.
- Runner OCI images.
- Python grader SDK packaging.
- Language and toolchain environments.
- Development shells.
- Checks and reproducible builds.
- Future Firecracker root filesystems or VM images.

Do not model production grading jobs as Nix builds. Dynamic student submissions should not be copied into the Nix store. Nix build sandboxing is not the right malicious-code security boundary, store paths can expose private submission data, and per-submission derivations would create unnecessary store and garbage-collection pressure.

The intended model is:

```text
Nix builds the environment.
The executor runs the dynamic job using that environment.
```

## FHS Compatibility

Nix's non-FHS layout is both a feature and a compatibility concern.

It is a feature because dependencies become explicit, host leakage is reduced, and grading environments are reproducible.

It is a compatibility concern because student build systems and language tooling may expect conventional paths such as `/bin/sh`, `/usr/bin/gcc`, `/usr/include`, or `/lib`.

The system should provide curated runner environments first. Some can be more pure Nix-style, and others can be explicitly FHS-compatible for assignments that need conventional Linux paths.

Runner environment definitions should make FHS compatibility explicit. In the initial Docker socket executor, the worker owns the default runner image and resource policy; assignment configuration can override those defaults when an assignment genuinely needs a different environment.

```toml
[runner]
environment = "c-python"
fhs = true
```

Custom assignment flakes or custom runner environments can be added later after the curated runner model is stable.

## Stable Job Filesystem Contract

All executor backends should provide the same runtime layout:

```text
/input
  metadata.json
  files/

/grader
  grade.py
  support files

/work
  writable scratch directory

/output
  result.json
  artifacts/
```

Rules:

- `/input` should be read-only where possible.
- `/grader` should be read-only where possible.
- `/work` is writable scratch space.
- `/output` is writable result and artifact space.
- The worker validates `/output/result.json` before accepting it.
- Job execution should not receive control-plane credentials.

## Python Grader SDK

Instructor-authored graders should be Python scripts using a small SDK. The SDK should hide CMSX multipart details and provide a pleasant interface for common grading operations. Graders expose a normal `main(submission)` function; importing a grader module must not execute grading.

Example:

```python
from cmsx_autograder import Result

def main(submission):
    result = Result(max_score=100)

    build = submission.run(["gcc", "main.c", "-o", "main"], timeout=10)
    result.check("compiles", build.ok, points=30, feedback=build.stderr)

    if build.ok:
        run = submission.run(["./main"], input="hello\n", timeout=5)
        result.check("correct output", "expected" in run.stdout, points=70)

    return result
```

The runner invokes graders explicitly:

```sh
python -m cmsx_autograder /grader/grade.py
```

The SDK imports `grade.py`, looks for `main(submission)`, executes it, validates the returned `Result`, and writes `/output/result.json`. If grader loading or execution fails, the SDK should write a structured `error` result and exit nonzero so the worker can distinguish infrastructure failures from ordinary failed submissions.

The SDK should provide:

- Explicit `main(submission)` entrypoint.
- No import-time execution.
- Submission metadata access.
- File lookup by uploaded filename and problem name.
- Command execution helpers.
- Timeout handling.
- stdout/stderr capture.
- Check and scoring helpers.
- Result construction.
- Structured status updates.
- JSON result writing.
- Structured error result writing when grader loading or execution fails.
- Friendly error handling.
- Optional helper libraries for common tasks such as PDF form parsing.

## Assignment Configuration

Assignments should define their local webhook identity, grading bundle, auth tokens, and any assignment-specific overrides. They should not be required to repeat deployment-level executor details such as the default Docker image, default memory limit, default CPU limit, PID limit, network policy, or read-only root policy. Those defaults belong to worker configuration because they are operational policy and may vary by worker deployment.

Assignments should not require a configured CMSX assignment ID; CMSX's submitted `assignment_id` is stored per submission as request metadata. The assignment slug plus a valid assignment auth token binds an incoming CMSX request to a local assignment.

For the common Python runner path, assignment `execution_config` and `runner_config` can be empty. Assignment-level config should be treated as optional overrides for exceptional cases, such as a longer timeout, extra memory, network access, or a custom runner image.

Example:

```toml
slug = "c-basics"
name = "C Basics"
max_score = 100

[execution]
timeout_seconds = 30
memory_mb = 512
cpus = 1
pids_limit = 128
network = false
max_log_bytes = 1048576
max_output_bytes = 1048576
max_artifact_bytes = 10485760

[runner]
environment = "c-python"
image = "ghcr.io/example/cmsx-runner-c-python@sha256:..."
fhs = true

[capabilities]
read_files = true
parse_pdf = false
compile_code = true
run_commands = true
execute_student_code = true
network = false
```

Assignment auth tokens are the CMSX webhook authentication mechanism and should be stored hashed, not in plaintext.

The preferred minimal assignment configuration should look closer to:

```toml
slug = "python-intro"
name = "Python Intro"
max_score = 100

[execution]
# Empty means use worker defaults.

[runner]
# Empty means use worker default runner image.

[capabilities]
read_files = true
run_commands = false
execute_student_code = false
network = false
```

Worker configuration should define the default Docker executor policy, for example:

```toml
[executor]
backend = "docker-socket"
workspace_root = "/srv/cmsx-worker/jobs"
grader_root = "/srv/cmsx-worker/assignments"
max_jobs = 2
keep_workspaces = false
docker_host = "unix:///var/run/docker.sock"
default_image = "cmsx-runner-python:latest"
default_timeout_seconds = 60
default_memory_mb = 512
default_cpus = 1
default_pids_limit = 128
default_network = false
```

## Capability Model

Assignments should declare intended capabilities. Capabilities help drive policy, UI warnings, executor selection, and future security decisions.

Potential capabilities:

```text
read_files
parse_pdf
run_commands
compile_code
execute_student_code
network
artifacts
```

Capabilities are advisory and policy-driving. They do not replace actual executor enforcement.

Suggested policy mapping:

```text
read_files only:
  in-worker allowed, Docker recommended

parse_pdf:
  Docker recommended because parsers process untrusted input

run_commands:
  Docker required initially

compile_code or execute_student_code:
  Docker required initially, Firecracker recommended in the future

network:
  explicit opt-in only
```

## Result Schema

The grader writes a versioned JSON result to `/output/result.json`.

Minimum shape:

```json
{
  "schema_version": "1",
  "status": "passed",
  "score": 85,
  "max_score": 100,
  "feedback": "Overall feedback",
  "tests": [
    {
      "name": "compilation",
      "status": "passed",
      "score": 30,
      "max_score": 30,
      "message": "Compiled successfully"
    }
  ],
  "artifacts": []
}
```

Allowed top-level statuses should include:

```text
passed
failed
error
cancelled
```

The worker or control plane should validate:

- Required fields.
- Score bounds.
- Valid status values.
- Maximum JSON size.
- Maximum feedback size.
- Maximum test count.
- Maximum artifact count and size.
- Artifact paths stay within `/output/artifacts`.

The result file is untrusted until validated.

## Job Events And Logs

Every job should produce a durable, ordered event stream in addition to its final result.

Event shape:

```json
{
  "job_id": "job_123",
  "sequence": 42,
  "timestamp": "2026-05-04T12:00:00Z",
  "type": "stdout",
  "stream": "stdout",
  "visibility": "staff",
  "message": "Compiling main.c...\n",
  "data": {}
}
```

Requirements:

- `sequence` is monotonically increasing per job.
- Events are durable enough for web UI replay.
- Workers upload events in batches.
- The control plane exposes event retrieval APIs.
- Future UI can consume the same event store through polling, SSE, or WebSockets.

Useful event types:

```text
job.queued
job.claimed
job.started
job.input.prepared
executor.started
executor.container.created
executor.container.started
stdout
stderr
grader.status
resource.sample
artifact.created
result.written
job.succeeded
job.failed
job.timeout
job.cancelled
cleanup.started
cleanup.finished
```

Streams should distinguish operational logs from grader and student output:

```text
stdout
stderr
worker
resource
```

Visibility should distinguish who can see an event:

```text
student
staff
internal
```

## Worker Heartbeats And Monitoring

Workers should periodically report status and capacity.

Example heartbeat:

```json
{
  "version": "0.1.0",
  "status": "online",
  "running_jobs": 2,
  "max_jobs": 4,
  "active_job_ids": ["0196a6d5-7f6b-7c72-9d19-c4c3dcbef410"]
}
```

The control plane should track worker online/offline state, version, current load, recent failures, capacity, and last heartbeat time. Heartbeat responses should reconcile active leased jobs by returning renewed, cancelled, and unknown job IDs. The worker should treat that response as authoritative for cancellation and lease ownership.

The control plane should not initially schedule based on advertised worker executor backends or runner environments. A worker is provisioned for a deployment and has one local executor configured by the operator. If heterogeneous workers become necessary later, backend and runner matching can be added without changing the job lease model.

## Cancellation

Cancellation should be part of the job model.

Flow:

```text
control plane marks cancellation requested
worker observes cancellation
executor kills the running job
worker emits job.cancelled
worker performs cleanup
control plane records final cancelled state
```

Cancellation is needed for runaway jobs, excessive logs, stuck containers, administrative intervention, and future UI controls.

## Data Model

Generated internal entity IDs should use UUID v7 by default. The system is append-heavy and time-oriented, so time-sortable UUIDs improve index locality and make recent submissions, jobs, events, and results easier to inspect without introducing a central sequence generator.

UUID v7 ordering is a convenience, not the source of truth for time. Models should still store explicit timestamp fields such as `created_at`, `received_at`, `queued_at`, and event `timestamp`. Job events should also keep a per-job monotonic `sequence`, and event replay should order by `(job_id, sequence)` rather than relying on UUID order.

The core data model should include:

```text
assignments
assignment_tokens
submissions
submission_files
job_events
workers
worker_heartbeats
worker_keys
worker_request_nonces
runner_environments
artifacts
```

Important fields:

```text
assignments:
  id
  slug
  name
  max_score
  execution_config
  runner_config
  capabilities
  created_at
  updated_at

assignment_tokens:
  id
  assignment_id
  token_hash
  created_at
  revoked_at

submissions:
  id
  assignment_id
  cmsx_group_id
  cmsx_assignment_id
  cmsx_assignment_name
  netids_raw
  netids_json
  received_at
  raw_metadata

submission_files:
  id
  submission_id
  problem_name
  cmsx_file_field_name
  original_filename
  safe_filename
  storage_path
  content_sha256
  size_bytes
  created_at

grading_jobs:
  id
  submission_id
  assignment_id
  worker_id
  status
  attempts
  max_attempts
  queued_at
  claimed_at
  started_at
  finished_at
  lease_expires_at
  last_heartbeat_at
  cancel_requested_at
  failure_reason
  failure_message
  failure_retryable

grading_results:
  id
  job_id
  status
  score
  max_score
  feedback
  tests_json
  result_json
  stdout_summary
  stderr_summary
  duration_ms
  created_at

job_events:
  id
  job_id
  sequence
  timestamp
  type
  stream
  visibility
  message
  data_json

workers:
  id
  name
  status
  version
  created_at
  last_seen_at

worker_keys:
  id
  worker_id
  public_key
  public_key_fingerprint
  created_at
  revoked_at

worker_request_nonces:
  worker_id
  jti
  expires_at

worker_heartbeats:
  id
  worker_id
  status
  version
  running_jobs
  max_jobs
  reported_at

artifacts:
  id
  job_id
  path
  name
  content_type
  size_bytes
  sha256
  visibility
  created_at
```

## API Shape

Initial control plane APIs should cover CMSX ingestion, worker coordination, job events, results, artifacts, and inspection.

Representative endpoints:

```text
POST /cmsx/a/{assignment_slug}/submit
POST /workers/heartbeat
POST /workers/jobs/claim
GET  /workers/jobs/{job_id}
GET  /workers/jobs/{job_id}/files/{file_id}
POST /workers/jobs/{job_id}/started
POST /workers/jobs/{job_id}/events
POST /workers/jobs/{job_id}/result
POST /workers/jobs/{job_id}/artifacts
POST /workers/jobs/{job_id}/failed
GET  /jobs/{job_id}
GET  /jobs/{job_id}/events
POST /jobs/{job_id}/cancel
GET  /assignments/{assignment_slug}
GET  /assignments/{assignment_slug}/submissions
GET  /submissions/{submission_id}/results
```

The web UI should later be built around these same APIs rather than inventing a separate model.

## Security Model

Trust boundaries:

```text
Control plane:
  trusted service

Worker:
  trusted infrastructure component

Docker socket worker:
  effectively host-root equivalent through Docker daemon access

Job container:
  initial isolation boundary for grade.py and student code

grade.py:
  trusted in instructor-owned self-hosted deployments
  potentially untrusted in future hosted or multi-tenant deployments

student submissions:
  untrusted
```

Security requirements:

- Never pass Docker socket into job containers.
- Never pass worker or control-plane tokens into job containers.
- Use fresh job containers for Docker execution.
- Disable network by default.
- Enforce CPU, memory, PID, wall-clock, log, output, and artifact limits.
- Sanitize uploaded filenames.
- Store files under generated paths, not trusted names.
- Validate all result output.
- Clean up after every job.
- Recommend dedicated worker hosts or VMs for Docker socket mode.

The Docker socket executor is a practical initial backend, not the final strongest isolation story. Stronger backends should be added through the executor interface.

## Future Web UI

The web UI should be built after the core model is stable. It should use the same assignment, job, result, worker, and event APIs that exist for non-UI operation.

Expected UI features:

- Assignment management.
- Token management.
- Runner environment selection.
- Capability selection.
- Grader script editing.
- Submission list.
- Job status timeline.
- Live logs from the event stream.
- Result dashboard.
- Artifact viewer and downloads.
- Worker monitoring.
- Cancellation and retry controls.
- Future CMSX grade publishing controls.

## Non-Goals For The Initial Core

The initial core should not require:

- Web UI.
- CMSX grade publishing.
- Firecracker execution.
- Rootless Docker or Podman.
- Multi-tenant paid-service hardening.
- Custom assignment flakes.
- Perfect hidden-test protection from student code inside the same job container.

These should remain compatible future directions, not initial blockers.
