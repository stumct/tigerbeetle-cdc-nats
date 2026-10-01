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

	startRun(t, cfg, newFakeSource(testEvent(10), testEvent(20)))

	want := []string{
		`tb_cdc_build_info{version="test"} 1`,
		"tb_cdc_lock_held 1",
		"tb_cdc_events_published_total 2",
		"tb_cdc_last_event_timestamp_seconds 0.000",
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
