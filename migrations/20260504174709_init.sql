CREATE TABLE assignments (
  id UUID PRIMARY KEY,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  max_score DOUBLE PRECISION NOT NULL CHECK (
    max_score >= 0 AND max_score != 'NaN'::DOUBLE PRECISION
  ),
  execution_config JSONB NOT NULL,
  runner_config JSONB NOT NULL,
  capabilities JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_assignments_slug ON assignments(slug);

CREATE TABLE assignment_tokens (
  id UUID PRIMARY KEY,
  assignment_id UUID NOT NULL REFERENCES assignments(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ
);

CREATE INDEX idx_assignment_tokens_assignment_id
ON assignment_tokens(assignment_id);

CREATE TABLE workers (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('online', 'offline', 'disabled')
  ),
  version TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_workers_name ON workers(name);

CREATE TABLE worker_keys (
  id UUID PRIMARY KEY,
  worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  public_key TEXT NOT NULL,
  public_key_fingerprint TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ
);

CREATE INDEX idx_worker_keys_worker_id
ON worker_keys(worker_id);

CREATE UNIQUE INDEX idx_worker_keys_fingerprint_active
ON worker_keys(public_key_fingerprint)
WHERE revoked_at IS NULL;

CREATE TABLE worker_request_nonces (
  worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  jti UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (worker_id, jti)
);

CREATE INDEX idx_worker_request_nonces_expires_at
ON worker_request_nonces(expires_at);

CREATE TABLE worker_heartbeats (
  id UUID PRIMARY KEY,
  worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (
    status IN ('online', 'offline', 'disabled')
  ),
  version TEXT,
  running_jobs INTEGER NOT NULL CHECK (running_jobs >= 0),
  max_jobs INTEGER NOT NULL CHECK (max_jobs >= 0),
  reported_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_worker_heartbeats_worker_reported
ON worker_heartbeats(worker_id, reported_at DESC);

CREATE TABLE runner_environments (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  image TEXT,
  fhs BOOLEAN NOT NULL DEFAULT false,
  metadata JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_runner_environments_name
ON runner_environments(name);

CREATE TABLE submissions (
  id UUID PRIMARY KEY,
  assignment_id UUID NOT NULL REFERENCES assignments(id) ON DELETE RESTRICT,
  cmsx_group_id TEXT NOT NULL,
  cmsx_assignment_id TEXT NOT NULL,
  cmsx_assignment_name TEXT NOT NULL,
  netids_raw TEXT NOT NULL,
  netids_json JSONB,
  received_at TIMESTAMPTZ NOT NULL,
  raw_metadata JSONB NOT NULL
);

CREATE INDEX idx_submissions_assignment_received
ON submissions(assignment_id, received_at DESC);

CREATE TABLE submission_files (
  id UUID PRIMARY KEY,
  submission_id UUID NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
  problem_name TEXT,
  cmsx_file_field_name TEXT NOT NULL,
  original_filename TEXT NOT NULL,
  safe_filename TEXT NOT NULL,
  storage_path TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_submission_files_submission_id
ON submission_files(submission_id);

CREATE UNIQUE INDEX idx_submission_files_submission_safe_filename
ON submission_files(submission_id, safe_filename);

CREATE TABLE grading_jobs (
  id UUID PRIMARY KEY,
  submission_id UUID NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
  assignment_id UUID NOT NULL REFERENCES assignments(id) ON DELETE RESTRICT,
  worker_id UUID REFERENCES workers(id) ON DELETE SET NULL,
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'error', 'cancelled')
  ),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
  queued_at TIMESTAMPTZ NOT NULL,
  claimed_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  cancel_requested_at TIMESTAMPTZ,
  failure_reason TEXT,
  failure_message TEXT,
  failure_retryable BOOLEAN
);

CREATE INDEX idx_grading_jobs_status_queued
ON grading_jobs(status, queued_at);

CREATE INDEX idx_grading_jobs_submission_id
ON grading_jobs(submission_id);

CREATE INDEX idx_grading_jobs_assignment_id
ON grading_jobs(assignment_id);

CREATE INDEX idx_grading_jobs_worker_id
ON grading_jobs(worker_id);

CREATE INDEX idx_grading_jobs_lease_expiry
ON grading_jobs(status, lease_expires_at);

CREATE INDEX idx_grading_jobs_worker_status
ON grading_jobs(worker_id, status);

CREATE TABLE grading_results (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES grading_jobs(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (
    status IN ('passed', 'failed', 'error', 'cancelled')
  ),
  score DOUBLE PRECISION NOT NULL CHECK (
    score >= 0 AND score != 'NaN'::DOUBLE PRECISION
  ),
  max_score DOUBLE PRECISION NOT NULL CHECK (
    max_score >= 0 AND max_score != 'NaN'::DOUBLE PRECISION
  ),
  feedback TEXT,
  tests JSONB NOT NULL,
  result JSONB NOT NULL,
  stdout_summary TEXT,
  stderr_summary TEXT,
  duration_ms BIGINT CHECK (duration_ms IS NULL OR duration_ms >= 0),
  created_at TIMESTAMPTZ NOT NULL,
  CHECK (score <= max_score)
);

CREATE UNIQUE INDEX idx_grading_results_job_id
ON grading_results(job_id);

CREATE TABLE job_events (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES grading_jobs(id) ON DELETE CASCADE,
  sequence BIGINT NOT NULL CHECK (sequence >= 0),
  timestamp TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL,
  stream TEXT NOT NULL CHECK (
    stream IN ('stdout', 'stderr', 'worker', 'resource')
  ),
  visibility TEXT NOT NULL CHECK (
    visibility IN ('student', 'staff', 'internal')
  ),
  message TEXT NOT NULL,
  data JSONB NOT NULL
);

CREATE UNIQUE INDEX idx_job_events_job_sequence
ON job_events(job_id, sequence);

CREATE INDEX idx_job_events_job_timestamp
ON job_events(job_id, timestamp);

CREATE TABLE artifacts (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES grading_jobs(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  name TEXT NOT NULL,
  content_type TEXT,
  size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
  sha256 TEXT NOT NULL,
  visibility TEXT NOT NULL CHECK (
    visibility IN ('student', 'staff', 'internal')
  ),
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_artifacts_job_id
ON artifacts(job_id);


