CREATE TABLE assignments (
  id TEXT NOT NULL PRIMARY KEY,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  cmsx_assignment_id TEXT NOT NULL,
  max_score DOUBLE PRECISION NOT NULL CHECK (max_score >= 0),
  execution_config_json TEXT NOT NULL,
  runner_config_json TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_assignments_slug ON assignments(slug);

CREATE TABLE assignment_tokens (
  id TEXT NOT NULL PRIMARY KEY,
  assignment_id TEXT NOT NULL REFERENCES assignments(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT
);

CREATE INDEX idx_assignment_tokens_assignment_id
ON assignment_tokens(assignment_id);

CREATE TABLE submissions (
  id TEXT NOT NULL PRIMARY KEY,
  assignment_id TEXT NOT NULL REFERENCES assignments(id) ON DELETE RESTRICT,
  cmsx_assignment_id TEXT NOT NULL,
  cmsx_assignment_name TEXT NOT NULL,
  netids_raw TEXT NOT NULL,
  received_at TEXT NOT NULL,
  raw_metadata_json TEXT NOT NULL
);

CREATE INDEX idx_submissions_assignment_received
ON submissions(assignment_id, received_at);

CREATE TABLE submission_files (
  id TEXT NOT NULL PRIMARY KEY,
  submission_id TEXT NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
  problem_name TEXT,
  cmsx_file_field_name TEXT NOT NULL,
  original_filename TEXT NOT NULL,
  storage_path TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  created_at TEXT NOT NULL
);

CREATE INDEX idx_submission_files_submission_id
ON submission_files(submission_id);

CREATE TABLE grading_jobs (
  id TEXT NOT NULL PRIMARY KEY,
  submission_id TEXT NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
  assignment_id TEXT NOT NULL REFERENCES assignments(id) ON DELETE RESTRICT,
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'running', 'succeeded', 'failed', 'error', 'cancelled')
  ),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  queued_at TEXT NOT NULL,
  claimed_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  cancel_requested_at TEXT,
  error_message TEXT
);

CREATE INDEX idx_grading_jobs_status_queued
ON grading_jobs(status, queued_at);

CREATE INDEX idx_grading_jobs_submission_id
ON grading_jobs(submission_id);

CREATE INDEX idx_grading_jobs_assignment_id
ON grading_jobs(assignment_id);

CREATE TABLE grading_results (
  id TEXT NOT NULL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES grading_jobs(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (
    status IN ('passed', 'failed', 'error', 'cancelled')
  ),
  score DOUBLE PRECISION NOT NULL CHECK (score >= 0),
  max_score DOUBLE PRECISION NOT NULL CHECK (max_score >= 0),
  feedback TEXT,
  tests_json TEXT NOT NULL,
  result_json TEXT NOT NULL,
  stdout_summary TEXT,
  stderr_summary TEXT,
  duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0),
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_grading_results_job_id
ON grading_results(job_id);

CREATE TABLE job_events (
  id TEXT NOT NULL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES grading_jobs(id) ON DELETE CASCADE,
  sequence INTEGER NOT NULL CHECK (sequence >= 0),
  timestamp TEXT NOT NULL,
  type TEXT NOT NULL,
  stream TEXT NOT NULL,
  visibility TEXT NOT NULL CHECK (
    visibility IN ('student', 'staff', 'internal')
  ),
  message TEXT NOT NULL,
  data_json TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_job_events_job_sequence
ON job_events(job_id, sequence);

CREATE INDEX idx_job_events_job_timestamp
ON job_events(job_id, timestamp);
