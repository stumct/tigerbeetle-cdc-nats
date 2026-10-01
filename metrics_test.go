package cdcnats

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRun_ServesMetrics(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg := testConfig(t, url, "--metrics-addr="+addr)

	// TigerBeetle timestamps are nanoseconds since the Unix epoch.
	startRun(t, cfg, newFakeSource(testEvent(1_759_300_000_000_000_000), testEvent(1_759_300_001_500_000_000)))

	want := []string{
		`tb_cdc_build_info{version="test"} 1`,
		"tb_cdc_lock_held 1",
		"tb_cdc_events_published_total 2",
		"tb_cdc_last_event_timestamp_seconds 1759300001.500",
		"tb_cdc_caught_up 1",
		"tb_cdc_publish_failures_total 0",
	}
	var body string
	eventually(t, 5*time.Second, "metrics to report the published events", func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		body = string(raw)
		for _, line := range want {
			if !strings.Contains(body, line+"\n") {
				return false
			}
		}
		return true
	})

	if !strings.Contains(body, "# TYPE tb_cdc_events_published_total counter\n") {
		t.Fatalf("metrics output lacks TYPE metadata:\n%s", body)
	}
}

func TestMetrics_EscapesLabelValues(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	newMetrics("v1 \"quoted\"\nline\\x").write(&out)
	want := `tb_cdc_build_info{version="v1 \"quoted\"\nline\\x"} 1`
	if !strings.Contains(out.String(), want+"\n") {
		t.Fatalf("metrics output lacks %s:\n%s", want, out.String())
	}
}

func TestRun_MetricsReportTheStoredEventNotTheTimestampFlag(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)
	runUntilPublished(t, js, cfg, newFakeSource(testEvent(1_000_000_000)), []uint64{1_000_000_000})

	// The flag is ignored for a stream that holds events, and the metric reports the stored event, not
	// the flag's timestamp.
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startRun(t, testConfig(t, url, "--timestamp-last=5000000000", "--metrics-addr="+addr), newFakeSource())

	eventually(t, 5*time.Second, "metrics to report the last stored event", func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		return err == nil &&
			strings.Contains(string(body), "tb_cdc_last_event_timestamp_seconds 1.000\n") &&
			strings.Contains(string(body), "tb_cdc_caught_up 1\n")
	})
}

func TestMetrics_CountEventsByStreamSequence(t *testing.T) {
	t.Parallel()

	m := newMetrics("test")
	m.recordResume(position{streamSeq: 10}) // events stored before this process started aren't counted
	m.recordStored(11, 110)
	m.recordStored(12, 120)
	// The acknowledgement for sequence 13 was lost; resuming finds the stream at 14.
	m.recordResume(position{streamSeq: 14, storedTimestamp: 140})
	m.recordStored(14, 140) // a late acknowledgement for an already counted event

	if got := m.eventsPublished.Load(); got != 4 {
		t.Fatalf("events published = %d, want 4", got)
	}
	if got := m.lastEventTimestamp.Load(); got != 140 {
		t.Fatalf("last event timestamp = %d, want 140", got)
	}
}

func TestMetrics_CountEventsWhoseAcksWereLostOnANewStream(t *testing.T) {
	t.Parallel()

	m := newMetrics("test")
	m.recordResume(position{streamSeq: 0})
	// Every acknowledgement of the first batch was lost; resuming finds 3 events stored.
	m.recordResume(position{streamSeq: 3, storedTimestamp: 30})

	if got := m.eventsPublished.Load(); got != 3 {
		t.Fatalf("events published = %d, want 3", got)
	}
}

func TestMetrics_CountEventsPublishedAgainAfterNATSLostSome(t *testing.T) {
	t.Parallel()

	m := newMetrics("test")
	m.recordResume(position{streamSeq: 100})
	// NATS lost acknowledged events 91-100; publishing resumes at sequence 90 and stores them again.
	m.recordResume(position{streamSeq: 90})
	for seq := uint64(91); seq <= 100; seq++ {
		m.recordStored(seq, seq*10)
	}

	if got := m.eventsPublished.Load(); got != 10 {
		t.Fatalf("events published = %d, want 10", got)
	}
}
