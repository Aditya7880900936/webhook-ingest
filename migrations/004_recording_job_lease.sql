ALTER TABLE recording_jobs
ADD COLUMN IF NOT EXISTS processing_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_recording_jobs_processing
ON recording_jobs (status, processing_at);