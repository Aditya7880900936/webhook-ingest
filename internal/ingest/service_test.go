package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveryIsIdempotent(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const requests = 10
	errCh := make(chan error, requests)

	for i := 0; i < requests; i++ {
		go func() {
			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				errCh <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("got status %d, want 200", resp.StatusCode)
				return
			}

			errCh <- nil
		}()
	}

	for i := 0; i < requests; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	var n int
	err := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Wait for async recording processing.
	deadline := time.Now().Add(2 * time.Second)

	var processed bool
	for time.Now().Before(deadline) {
		err := st.Pool().QueryRow(
			ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`,
			callID,
		).Scan(&processed)

		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		if processed {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("expected recording to be marked processed within 2 seconds")
}

func TestRecordingWorkerProcessesDurableJob(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Verify the webhook created a durable pending recording job.
	var status string
	err := st.Pool().QueryRow(ctx, `
		SELECT status
		FROM recording_jobs
		WHERE call_id = $1
	`, callID).Scan(&status)
	if err != nil {
		t.Fatalf("scan recording job: %v", err)
	}

	if status != "pending" {
		t.Fatalf("got recording job status %q, want pending", status)
	}

	// The worker should eventually claim and process the job.
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		err := st.Pool().QueryRow(ctx, `
			SELECT status
			FROM recording_jobs
			WHERE call_id = $1
		`, callID).Scan(&status)
		if err != nil {
			t.Fatalf("scan recording job: %v", err)
		}

		if status == "processed" {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("recording job was not processed within 2 seconds")
}

func TestRecordingWorkerProcessesStaleJob(t *testing.T) {
	_, st := testutil.NewServer(t)
	_, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Create the call record required by the recording_jobs foreign key.
	_, err := st.Pool().Exec(ctx, `
		INSERT INTO calls (
			call_id,
			account_id,
			status,
			duration_sec,
			recording_url,
			updated_at
		)
		VALUES ($1, $2, 'completed', 143, $3, now())
	`, callID, accountID, "https://recordings.example.com/test.wav")
	if err != nil {
		t.Fatalf("insert call: %v", err)
	}

	// Simulate a job that was being processed when the service died.
	_, err = st.Pool().Exec(ctx, `
		INSERT INTO recording_jobs (
			call_id,
			recording_url,
			status,
			processing_at
		)
		VALUES (
			$1,
			$2,
			'processing',
			now() - interval '2 minutes'
		)
	`, callID, "https://recordings.example.com/test.wav")
	if err != nil {
		t.Fatalf("insert stale recording job: %v", err)
	}

	// The worker should reclaim the stale job and eventually process it.

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		var status string

		err := st.Pool().QueryRow(ctx, `
			SELECT status
			FROM recording_jobs
			WHERE call_id = $1
		`, callID).Scan(&status)
		if err != nil {
			t.Fatalf("scan recording job: %v", err)
		}

		if status == "processed" {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("expected stale recording job to be recovered and processed")
}
