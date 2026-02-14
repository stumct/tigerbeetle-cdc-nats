package cdcnats

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

const (
	integrationEnv         = "TB_CDC_INTEGRATION"
	integrationExternalEnv = "TB_CDC_EXTERNAL_SERVICES"

	externalNATSURLEnv   = "TB_CDC_EXTERNAL_NATS_URL"
	externalClusterEnv   = "TB_CDC_EXTERNAL_CLUSTER_ID"
	externalAddressesEnv = "TB_CDC_EXTERNAL_ADDRESSES"

	natsServerBinEnv      = "NATS_SERVER_BIN"
	tigerbeetleBinEnv     = "TIGERBEETLE_BIN"
	startupTimeout        = 20 * time.Second
	publishTimeout        = 20 * time.Second
	runnerShutdownTimeout = 5 * time.Second
)

func TestIntegration_CDCResumeWithJetStreamState(t *testing.T) {
	if os.Getenv(integrationEnv) != "1" {
		t.Skipf("set %s=1 to run integration tests", integrationEnv)
	}

	suffix := randomSuffix()
	var (
		natsURL        string
		clusterDecimal string
		clusterID      types.Uint128
		addresses      []string
		err            error
	)

	if os.Getenv(integrationExternalEnv) == "1" {
		natsURL = requiredEnv(t, externalNATSURLEnv)
		clusterDecimal = requiredEnv(t, externalClusterEnv)
		clusterID, clusterDecimal, err = parseUint128Decimal(clusterDecimal)
		if err != nil {
			t.Fatalf("invalid %s=%q: %v", externalClusterEnv, clusterDecimal, err)
		}

		addresses, err = parseAddresses(requiredEnv(t, externalAddressesEnv))
		if err != nil {
			t.Fatalf("invalid %s: %v", externalAddressesEnv, err)
		}

		waitForNATS(t, natsURL, startupTimeout)
		waitForTigerBeetle(t, clusterID, addresses, startupTimeout)
	} else {
		natsServerBin := findRequiredBinary(t, natsServerBinEnv, "nats-server")
		tigerbeetleBin := findRequiredBinary(t, tigerbeetleBinEnv, "tigerbeetle")

		tmpDir := t.TempDir()

		natsPort := reserveTCPPort(t)
		natsStore := filepath.Join(tmpDir, "nats-store")
		if err := os.MkdirAll(natsStore, 0o755); err != nil {
			t.Fatalf("os.MkdirAll(%q): %v", natsStore, err)
		}
		natsURL = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
		_ = startBackgroundProcess(t, natsServerBin,
			"-js",
			"-a", "127.0.0.1",
			"-p", strconv.Itoa(natsPort),
			"-sd", natsStore,
		)
		waitForNATS(t, natsURL, startupTimeout)

		clusterDecimal = strconv.FormatInt(time.Now().UnixNano(), 10)
		clusterID, clusterDecimal, err = parseUint128Decimal(clusterDecimal)
		if err != nil {
			t.Fatalf("parseUint128Decimal(%q): %v", clusterDecimal, err)
		}

		tbPort := reserveTCPPort(t)
		tbAddress := fmt.Sprintf("127.0.0.1:%d", tbPort)
		addresses = []string{tbAddress}
		tbDataFile := filepath.Join(tmpDir, "0_0.tigerbeetle")

		runCommand(t, tigerbeetleBin,
			"format",
			"--cluster="+clusterDecimal,
			"--replica=0",
			"--replica-count=1",
			"--development",
			tbDataFile,
		)
		_ = startBackgroundProcess(t, tigerbeetleBin,
			"start",
			"--addresses="+tbAddress,
			"--development",
			tbDataFile,
		)
		waitForTigerBeetle(t, clusterID, addresses, startupTimeout)
	}

	producerClient, err := tigerbeetle_go.NewClient(clusterID, addresses)
	if err != nil {
		t.Fatalf("NewClient(): %v", err)
	}
	t.Cleanup(producerClient.Close)

	idBase := uint64(time.Now().UnixNano())
	debitAccountID := idBase
	creditAccountID := idBase + 1
	transferID1 := idBase + 100
	transferID2 := idBase + 101

	createAccounts(t, producerClient,
		types.Account{ID: types.ToUint128(debitAccountID), Ledger: 1, Code: 1},
		types.Account{ID: types.ToUint128(creditAccountID), Ledger: 1, Code: 1},
	)

	nc, err := nats.Connect(natsURL, nats.Name("tb-cdc-nats-integration-test"))
	if err != nil {
		t.Fatalf("nats.Connect(%q): %v", natsURL, err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nc.JetStream(): %v", err)
	}

	cfg := config{
		clusterID:              clusterID,
		clusterIDDecimal:       clusterDecimal,
		addresses:              addresses,
		natsURL:                natsURL,
		eventStream:            "TB_CDC_EVENTS_" + suffix,
		streamReplicas:         1,
		streamStorage:          nats.FileStorage,
		streamMaxAge:           0,
		streamMaxBytes:         -1,
		dedupeWindow:           2 * time.Minute,
		provision:              true,
		streamUpdate:           false,
		progressBucket:         "TB_CDC_PROGRESS_" + suffix,
		lockBucket:             "TB_CDC_LOCK_" + suffix,
		kvReplicas:             1,
		kvStorage:              nats.FileStorage,
		lockTTL:                6 * time.Second,
		lockRefresh:            2 * time.Second,
		subjectMode:            subjectModeStructured,
		subjectPrefix:          defaultSubjectPrefix,
		singleSubject:          defaultSingleSubject,
		publishMode:            publishModeAsync,
		publishAckTimeout:      10 * time.Second,
		publishAsyncMaxPending: 256,
		eventCountMax:          128,
		progressEveryEvents:    0,
		idleInterval:           100 * time.Millisecond,
		requestsPerSecondLimit: 0,
		timestampLast:          nil,
		version:                "integration-test",
	}

	run1Cancel, run1ErrCh := startRunner(t, cfg)

	createTransfers(t, producerClient,
		types.Transfer{
			ID:              types.ToUint128(transferID1),
			DebitAccountID:  types.ToUint128(debitAccountID),
			CreditAccountID: types.ToUint128(creditAccountID),
			Amount:          types.ToUint128(10),
			Ledger:          1,
			Code:            1,
		},
	)

	waitForStreamMessageCount(t, js, cfg.eventStream, 1, publishTimeout, run1ErrCh)
	assertRunnerHasNoError(t, run1ErrCh)

	msg1 := getStreamMessage(t, js, cfg.eventStream, 1)
	assertEventMessageMetadata(t, cfg, msg1)
	payload1 := decodeEventPayload(t, msg1.Data)
	if got, want := payload1.TransferID, strconv.FormatUint(transferID1, 10); got != want {
		t.Fatalf("first event transfer.id = %q, want %q", got, want)
	}

	waitForProgressTimestamp(t, js, cfg.progressBucket, cfg.progressKey(), payload1.Timestamp, publishTimeout)

	stopRunner(t, run1Cancel, run1ErrCh)

	createTransfers(t, producerClient,
		types.Transfer{
			ID:              types.ToUint128(transferID2),
			DebitAccountID:  types.ToUint128(debitAccountID),
			CreditAccountID: types.ToUint128(creditAccountID),
			Amount:          types.ToUint128(25),
			Ledger:          1,
			Code:            1,
		},
	)

	run2Cancel, run2ErrCh := startRunner(t, cfg)
	waitForStreamMessageCount(t, js, cfg.eventStream, 2, publishTimeout, run2ErrCh)
	assertRunnerHasNoError(t, run2ErrCh)

	msg2 := getStreamMessage(t, js, cfg.eventStream, 2)
	assertEventMessageMetadata(t, cfg, msg2)
	payload2 := decodeEventPayload(t, msg2.Data)
	if got, want := payload2.TransferID, strconv.FormatUint(transferID2, 10); got != want {
		t.Fatalf("second event transfer.id = %q, want %q", got, want)
	}
	if payload2.Timestamp <= payload1.Timestamp {
		t.Fatalf("expected increasing timestamps, got first=%d second=%d", payload1.Timestamp, payload2.Timestamp)
	}

	waitForProgressTimestamp(t, js, cfg.progressBucket, cfg.progressKey(), payload2.Timestamp, publishTimeout)

	time.Sleep(500 * time.Millisecond)
	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(%q): %v", cfg.eventStream, err)
	}
	if got, want := info.State.Msgs, uint64(2); got != want {
		t.Fatalf("unexpected message count after restart: got %d want %d", got, want)
	}

	stopRunner(t, run2Cancel, run2ErrCh)
}

func startRunner(t *testing.T, cfg config) (func(), chan error) {
	t.Helper()

	runErrCh := make(chan error, 1)
	runnerDone := make(chan struct{})
	ctxDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(runnerDone)
		runErrCh <- run(ctx, cfg)
	}()

	stop := func() {
		cancel()
		select {
		case <-runnerDone:
		case <-time.After(runnerShutdownTimeout):
			t.Fatalf("runner did not shut down within %s", runnerShutdownTimeout)
		}
		close(ctxDone)
	}

	t.Cleanup(func() {
		select {
		case <-ctxDone:
		default:
			stop()
		}
	})

	return stop, runErrCh
}

func stopRunner(t *testing.T, stop func(), runErrCh chan error) {
	t.Helper()

	stop()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("runner stopped with error: %v", err)
		}
	case <-time.After(runnerShutdownTimeout):
		t.Fatalf("timed out waiting for runner stop")
	}
}

func assertRunnerHasNoError(t *testing.T, runErrCh chan error) {
	t.Helper()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("runner exited early with error: %v", err)
		}
		t.Fatalf("runner exited early")
	default:
	}
}

func waitForNATS(t *testing.T, natsURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(natsURL, nats.MaxReconnects(0), nats.Timeout(250*time.Millisecond))
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		js, err := nc.JetStream()
		if err == nil {
			_, err = js.AccountInfo()
		}
		nc.Close()

		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for NATS at %q: %v", natsURL, lastErr)
}

func waitForTigerBeetle(t *testing.T, clusterID types.Uint128, addresses []string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := tigerbeetle_go.NewClient(clusterID, addresses)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err = client.Nop()
		client.Close()
		if err == nil {
			return
		}

		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for TigerBeetle: %v", lastErr)
}

func createAccounts(t *testing.T, client tigerbeetle_go.Client, accounts ...types.Account) {
	t.Helper()

	results, err := client.CreateAccounts(accounts)
	if err != nil {
		t.Fatalf("CreateAccounts(): %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("CreateAccounts() returned errors: %+v", results)
	}
}

func createTransfers(t *testing.T, client tigerbeetle_go.Client, transfers ...types.Transfer) {
	t.Helper()

	results, err := client.CreateTransfers(transfers)
	if err != nil {
		t.Fatalf("CreateTransfers(): %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("CreateTransfers() returned errors: %+v", results)
	}
}

func waitForStreamMessageCount(
	t *testing.T,
	js nats.JetStreamContext,
	stream string,
	count uint64,
	timeout time.Duration,
	runErrCh <-chan error,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if runErrCh != nil {
			select {
			case err := <-runErrCh:
				if err != nil {
					t.Fatalf("runner exited while waiting for stream messages: %v", err)
				}
				t.Fatalf("runner exited while waiting for stream messages")
			default:
			}
		}

		info, err := js.StreamInfo(stream)
		if err == nil {
			if info.State.Msgs >= count {
				return
			}
			lastErr = fmt.Errorf("current messages=%d", info.State.Msgs)
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for stream %q to reach %d messages: %v", stream, count, lastErr)
}

func getStreamMessage(t *testing.T, js nats.JetStreamContext, stream string, seq uint64) *nats.RawStreamMsg {
	t.Helper()

	msg, err := js.GetMsg(stream, seq)
	if err != nil {
		t.Fatalf("GetMsg(stream=%q, seq=%d): %v", stream, seq, err)
	}
	return msg
}

func waitForProgressTimestamp(
	t *testing.T,
	js nats.JetStreamContext,
	bucket string,
	key string,
	expected uint64,
	timeout time.Duration,
) {
	t.Helper()

	kv, err := js.KeyValue(bucket)
	if err != nil {
		t.Fatalf("KeyValue(%q): %v", bucket, err)
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		entry, err := kv.Get(key)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				lastErr = err
				time.Sleep(100 * time.Millisecond)
				continue
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var progress progressRecord
		if err := json.Unmarshal(entry.Value(), &progress); err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if progress.Timestamp >= expected {
			return
		}

		lastErr = fmt.Errorf("progress timestamp=%d", progress.Timestamp)
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for progress key %q >= %d: %v", key, expected, lastErr)
}

type decodedEventPayload struct {
	Timestamp  uint64
	TransferID string
}

func decodeEventPayload(t *testing.T, raw []byte) decodedEventPayload {
	t.Helper()

	var payload struct {
		Timestamp any `json:"timestamp"`
		Transfer  struct {
			ID any `json:"id"`
		} `json:"transfer"`
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}

	timestamp, err := jsonValueToUint64(payload.Timestamp)
	if err != nil {
		t.Fatalf("parse timestamp from payload: %v", err)
	}

	transferID, err := jsonValueToDecimalString(payload.Transfer.ID)
	if err != nil {
		t.Fatalf("parse transfer.id from payload: %v", err)
	}

	return decodedEventPayload{
		Timestamp:  timestamp,
		TransferID: transferID,
	}
}

func jsonValueToUint64(value any) (uint64, error) {
	switch v := value.(type) {
	case json.Number:
		if intValue, err := v.Int64(); err == nil {
			if intValue < 0 {
				return 0, fmt.Errorf("negative value: %d", intValue)
			}
			return uint64(intValue), nil
		}
		uintValue, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return 0, err
		}
		return uintValue, nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative value: %v", v)
		}
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unsupported JSON number type %T", value)
	}
}

func jsonValueToDecimalString(value any) (string, error) {
	switch v := value.(type) {
	case json.Number:
		return v.String(), nil
	case string:
		return v, nil
	case float64:
		return strconv.FormatUint(uint64(v), 10), nil
	default:
		return "", fmt.Errorf("unsupported JSON number type %T", value)
	}
}

func assertEventMessageMetadata(t *testing.T, cfg config, msg *nats.RawStreamMsg) {
	t.Helper()

	expectedSubject := cfg.subjectForEvent(1, "single_phase")
	if got := msg.Subject; got != expectedSubject {
		t.Fatalf("message subject = %q, want %q", got, expectedSubject)
	}

	if got := msg.Header.Get("event_type"); got != "single_phase" {
		t.Fatalf("header event_type = %q, want %q", got, "single_phase")
	}
	if got := msg.Header.Get("ledger"); got != "1" {
		t.Fatalf("header ledger = %q, want %q", got, "1")
	}
	if got := msg.Header.Get("transfer_code"); got != "1" {
		t.Fatalf("header transfer_code = %q, want %q", got, "1")
	}
	if got := msg.Header.Get("debit_account_code"); got != "1" {
		t.Fatalf("header debit_account_code = %q, want %q", got, "1")
	}
	if got := msg.Header.Get("credit_account_code"); got != "1" {
		t.Fatalf("header credit_account_code = %q, want %q", got, "1")
	}
}

func requiredEnv(t *testing.T, key string) string {
	t.Helper()

	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Fatalf("%s must be set when %s=1", key, integrationExternalEnv)
	}

	return value
}

func findRequiredBinary(t *testing.T, envName string, fallback string) string {
	t.Helper()

	if fromEnv := strings.TrimSpace(os.Getenv(envName)); fromEnv != "" {
		if _, err := os.Stat(fromEnv); err != nil {
			t.Fatalf("%s=%q is not accessible: %v", envName, fromEnv, err)
		}
		return fromEnv
	}

	path, err := exec.LookPath(fallback)
	if err != nil {
		t.Skipf("required binary %q not found in PATH and %s is unset", fallback, envName)
	}
	return path
}

func runCommand(t *testing.T, binary string, args ...string) {
	t.Helper()

	cmd := exec.Command(binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %s %s\nerror: %v\noutput:\n%s", binary, strings.Join(args, " "), err, output)
	}
}

func startBackgroundProcess(t *testing.T, binary string, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(binary, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("start process %s %s: %v", binary, strings.Join(args, " "), err)
	}

	t.Cleanup(func() {
		if err := stopProcess(cmd); err != nil {
			t.Fatalf("stop process %s %s: %v\nprocess output:\n%s", binary, strings.Join(args, " "), err, output.String())
		}
	})

	return cmd
}

func stopProcess(cmd *exec.Cmd) error {
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return nil
	}

	if cmd.Process == nil {
		return nil
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err == nil {
			return nil
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil
		}
		return err
	case <-time.After(5 * time.Second):
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		<-done
		return nil
	}
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func randomSuffix() string {
	var bytes [4]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return strings.ToUpper(hex.EncodeToString(bytes[:]))
	}

	return strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
}
