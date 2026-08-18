CREATE TABLE IF NOT EXISTS recording_jobs (
    call_id        TEXT PRIMARY KEY REFERENCES calls(call_id),
    recording_url  TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_at  TIMESTAMPTZ,
    processed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_recording_jobs_pending
ON recording_jobs (status, created_at);