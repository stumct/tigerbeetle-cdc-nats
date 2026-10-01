package cdcnats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// metrics are the publisher's Prometheus metrics. The runner updates them; scrapes of /metrics read
// them. All fields are safe for concurrent use.
type metrics struct {
	version string

	lockHeld        atomic.Bool
	eventsPublished atomic.Uint64
	publishFailures atomic.Uint64
	// lastEventTimestamp is the TigerBeetle timestamp (nanoseconds) of the last published event.
	lastEventTimestamp atomic.Uint64
	// lastPollUnixNano is the wall-clock time of the last successful TigerBeetle query.
	lastPollUnixNano atomic.Int64
	// caughtUp is set when the last query returned no events. TigerBeetle caps each response below the
	// requested limit, so a short batch doesn't mean the publisher has caught up.
	caughtUp atomic.Bool
}

func newMetrics(version string) *metrics {
	return &metrics{version: version}
}

// recordPoll records a successful TigerBeetle query that returned n events.
func (m *metrics) recordPoll(n int) {
	m.lastPollUnixNano.Store(time.Now().UnixNano())
	m.caughtUp.Store(n == 0)
}

// recordStored records an event JetStream confirmed it stored.
func (m *metrics) recordStored(timestamp uint64) {
	m.eventsPublished.Add(1)
	m.lastEventTimestamp.Store(timestamp)
}

// ServeHTTP writes the metrics in the Prometheus text exposition format.
func (m *metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	m.write(w)
}

func (m *metrics) write(w io.Writer) {
	writeMetric(w, "tb_cdc_build_info", "gauge", "Publisher version.",
		`{version="`+labelEscaper.Replace(m.version)+`"}`, "1")
	writeMetric(w, "tb_cdc_lock_held", "gauge", "1 while this instance holds the single-writer lock.",
		"", boolValue(m.lockHeld.Load()))
	writeMetric(w, "tb_cdc_events_published_total", "counter", "Change events published to the stream.",
		"", strconv.FormatUint(m.eventsPublished.Load(), 10))
	writeMetric(w, "tb_cdc_publish_failures_total", "counter", "Failed publishes or resumes, each retried from the stream.",
		"", strconv.FormatUint(m.publishFailures.Load(), 10))
	writeMetric(w, "tb_cdc_last_event_timestamp_seconds", "gauge", "TigerBeetle timestamp of the last published event.",
		"", seconds(int64(m.lastEventTimestamp.Load())))
	writeMetric(w, "tb_cdc_last_poll_timestamp_seconds", "gauge", "Time of the last successful TigerBeetle query.",
		"", seconds(m.lastPollUnixNano.Load()))
	writeMetric(w, "tb_cdc_caught_up", "gauge", "1 if the last TigerBeetle query found no new events.",
		"", boolValue(m.caughtUp.Load()))
}

// labelEscaper escapes a label value as the Prometheus text format requires.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func writeMetric(w io.Writer, name, kind, help, labels, value string) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s%s %s\n", name, help, name, kind, name, labels, value)
}

func boolValue(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// seconds formats a Unix time in nanoseconds as seconds.
func seconds(unixNano int64) string {
	return strconv.FormatFloat(float64(unixNano)/float64(time.Second), 'f', 3, 64)
}

// serveMetrics serves the metrics at /metrics on addr until ctx is done. It returns once the
// listener is open, so a bad address fails startup.
func serveMetrics(ctx context.Context, addr string, m *metrics) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for metrics on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m)
	// Bound every phase of a request, so slow or stalled clients can't hold connections open.
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("warning: metrics server stopped: %v", err)
		}
	}()
	context.AfterFunc(ctx, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
		}
	})

	log.Printf("serving metrics at http://%s/metrics", listener.Addr())
	return nil
}
