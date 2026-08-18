// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	workerOnce sync.Once
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{
		store: s,
		cache: c,
		rdb:   rdb,
		log:   log,
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}

	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	return nil
}

// StartRecordingWorker starts the background worker that processes durable
// recording jobs from PostgreSQL.
func (s *Service) StartRecordingWorker(ctx context.Context) {
	s.workerOnce.Do(func() {
		go s.recordingWorker(ctx)
	})
}

// recordingWorker continuously checks PostgreSQL for pending recording jobs.
func (s *Service) recordingWorker(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := s.processPendingRecordingJobs(ctx); err != nil {
			s.log.Error("recording worker failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processPendingRecordingJobs loads and processes all currently pending jobs.
func (s *Service) processPendingRecordingJobs(ctx context.Context) error {
	jobs, err := s.store.PendingRecordingJobs(ctx)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := s.processRecording(ctx, job); err != nil {
			s.log.Error(
				"recording processing failed",
				"call_id", job.CallID,
				"error", err,
			)
			continue
		}

		if err := s.store.MarkRecordingJobProcessed(ctx, job.CallID); err != nil {
			s.log.Error(
				"mark recording job processed failed",
				"call_id", job.CallID,
				"error", err,
			)
		}
	}

	return nil
}

// processRecording downloads and transcodes the call recording.
func (s *Service) processRecording(ctx context.Context, job store.RecordingJob) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(recordingWork):
		return nil
	}
}
