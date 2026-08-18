# Solution

## What was broken and why

The service had several reliability issues.

First, webhook deliveries were not safely handled as a single atomic operation. A duplicate
delivery could cause the call statistics to be incremented more than once. The provider uses
at-least-once delivery semantics, so retries and redeliveries are expected.

I fixed this by using PostgreSQL as the source of truth for deduplication. `event_id` is unique
in the `events` table, and ingestion inserts the event with `ON CONFLICT (event_id) DO NOTHING`.
The event insert, call upsert, account statistics update, and recording-job creation are
performed in one PostgreSQL transaction. If the event was already inserted, the transaction
does not update the statistics again.

Recording processing was also not durable. A recording that was being processed when the
service stopped could remain stuck. Recording work is now represented by a durable
`recording_jobs` row. Workers claim jobs using `FOR UPDATE SKIP LOCKED`, mark them as
`processing`, and record `processing_at`. Jobs whose processing timestamp is stale can be
claimed again, allowing recovery after a worker or process failure.

The recording job and the corresponding call are marked processed in one transaction, so
the durable job state and `calls.recording_processed` cannot diverge.

## Deduplication strategy

I chose PostgreSQL rather than Redis for deduplication because the event and its business
side effects are already stored in PostgreSQL. Keeping deduplication in the same database
transaction makes the event insert and statistics update atomic. Redis could be used as a
fast deduplication cache, but it would introduce another consistency boundary and would not
replace the database uniqueness constraint.

## Scaling to 10,000 webhooks/second

At 10,000 webhooks/second I would keep PostgreSQL as the durable source of truth but move
the asynchronous processing behind a queue or streaming system. Webhook handlers should
remain lightweight and acknowledge after durable acceptance. Multiple worker instances
could consume recording jobs concurrently.

I would also review PostgreSQL partitioning/indexes, connection-pool sizing, batching,
backpressure, and observability. Redis could be introduced as a performance optimization,
but not as the sole correctness mechanism for deduplication.