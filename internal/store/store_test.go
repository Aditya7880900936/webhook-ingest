package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	inserted, err := s.InsertEvent(ctx, evt)
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if !inserted {
		t.Fatal("expected event to be inserted")
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

func TestClaimRecordingJobs(t *testing.T) {
	s := testutil.NewStore(t)
	_, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/test.wav",
	}

	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}

	_, err := s.Pool().Exec(ctx, `
		INSERT INTO recording_jobs (call_id, recording_url)
		VALUES ($1, $2)
	`, callID, evt.RecordingURL)
	if err != nil {
		t.Fatalf("insert recording job: %v", err)
	}

	jobs, err := s.ClaimRecordingJobs(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimRecordingJobs: %v", err)
	}

	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	if jobs[0].CallID != callID {
		t.Fatalf("got call ID %q, want %q", jobs[0].CallID, callID)
	}

	if jobs[0].RecordingURL != evt.RecordingURL {
		t.Fatalf(
			"got recording URL %q, want %q",
			jobs[0].RecordingURL,
			evt.RecordingURL,
		)
	}

	var status string
	err = s.Pool().QueryRow(ctx, `
		SELECT status
		FROM recording_jobs
		WHERE call_id = $1
	`, callID).Scan(&status)
	if err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "processing" {
		t.Fatalf("got status %q, want processing", status)
	}
}

func TestClaimRecordingJobsDoesNotReclaimJob(t *testing.T) {
	s := testutil.NewStore(t)
	_, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/test.wav",
	}

	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}

	_, err := s.Pool().Exec(ctx, `
		INSERT INTO recording_jobs (call_id, recording_url)
		VALUES ($1, $2)
	`, callID, evt.RecordingURL)
	if err != nil {
		t.Fatalf("insert recording job: %v", err)
	}

	first, err := s.ClaimRecordingJobs(ctx, 10)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	if len(first) != 1 {
		t.Fatalf("first claim got %d jobs, want 1", len(first))
	}

	second, err := s.ClaimRecordingJobs(ctx, 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}

	if len(second) != 0 {
		t.Fatalf("second claim got %d jobs, want 0", len(second))
	}
}

func TestMarkRecordingJobProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	_, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/test.wav",
	}

	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}

	_, err := s.Pool().Exec(ctx, `
		INSERT INTO recording_jobs (call_id, recording_url)
		VALUES ($1, $2)
	`, callID, evt.RecordingURL)
	if err != nil {
		t.Fatalf("insert recording job: %v", err)
	}

	if err := s.MarkRecordingJobProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingJobProcessed: %v", err)
	}

	var status string
	var processedAt *time.Time

	err = s.Pool().QueryRow(ctx, `
		SELECT status, processed_at
		FROM recording_jobs
		WHERE call_id = $1
	`, callID).Scan(&status, &processedAt)
	if err != nil {
		t.Fatalf("scan recording job: %v", err)
	}

	if status != "processed" {
		t.Fatalf("got status %q, want processed", status)
	}

	if processedAt == nil {
		t.Fatal("expected processed_at to be set")
	}

	var recordingProcessed bool
	err = s.Pool().QueryRow(ctx, `
		SELECT recording_processed
		FROM calls
		WHERE call_id = $1
	`, callID).Scan(&recordingProcessed)
	if err != nil {
		t.Fatalf("scan call: %v", err)
	}

	if !recordingProcessed {
		t.Fatal("expected call recording_processed to be true")
	}
}
