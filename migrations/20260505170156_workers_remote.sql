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

ALTER TABLE grading_jobs
ADD COLUMN lease_expires_at TIMESTAMPTZ,
ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

ALTER TABLE grading_jobs
DROP CONSTRAINT grading_jobs_status_check;

ALTER TABLE grading_jobs
ADD CONSTRAINT grading_jobs_status_check CHECK (
  status IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'error', 'cancelled')
);

CREATE INDEX idx_grading_jobs_lease_expiry
ON grading_jobs(status, lease_expires_at);

CREATE INDEX idx_grading_jobs_worker_status
ON grading_jobs(worker_id, status);
