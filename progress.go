package cdcnats

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

// errCannotResume marks conditions where the publisher can't tell where to resume, or resuming would
// corrupt the stream. Retrying won't help: an operator has to decide.
var errCannotResume = errors.New("cannot resume publishing")

// errTailMissing reports that the message at the stream's last sequence no longer exists.
var errTailMissing = errors.New("the stream's last message no longer exists")

// progressRecord is the checkpoint stored in the progress KV bucket after each published batch:
// the last published event's timestamp, and the stream sequence it was stored at.
type progressRecord struct {
	Timestamp uint64 `json:"timestamp"`
	StreamSeq uint64 `json:"stream_seq"`
	Version   string `json:"version"`
}

// position is where publishing resumes: after TigerBeetle timestamp `timestamp`, appending to the
// event stream after sequence `streamSeq`.
type position struct {
	timestamp uint64
	streamSeq uint64
}

// recoverPosition decides where publishing resumes. Reads go to the stream leaders, never to
// possibly stale replicas.
//
// The event stream is the record of what was published: publishing continues after the stream's
// last event, the message at its last sequence. That holds even if the previous run crashed before
// checkpointing, or a checkpoint outlived events NATS lost.
//
// If retention has removed every event, the progress checkpoint is used, but only if it was
// written for the stream's last sequence. Otherwise it can't say which events followed it.
//
// --timestamp-last starts publishing after the given timestamp. In a stream that holds events it
// only moves the start forward, because going back would duplicate and reorder events. In a stream
// without events it is the operator's choice, and wins over the checkpoint.
func recoverPosition(js nats.JetStreamContext, cfg config) (position, error) {
	lastSeq, timestamp, hasEvents, err := readTail(js, cfg)
	if err != nil {
		return position{}, err
	}
	override := cfg.timestampLast

	if hasEvents {
		switch {
		case override != nil && *override > timestamp:
			log.Printf("starting after --timestamp-last=%d, past the stream's last event (timestamp %d)", *override, timestamp)
			return position{timestamp: *override, streamSeq: lastSeq}, nil
		case override != nil:
			log.Printf("ignoring --timestamp-last=%d: the stream already holds events up to timestamp %d", *override, timestamp)
		}
		log.Printf("resuming after the stream's last event: timestamp=%d stream_seq=%d", timestamp, lastSeq)
		return position{timestamp: timestamp, streamSeq: lastSeq}, nil
	}

	if override != nil {
		log.Printf("stream %q holds no events; starting after --timestamp-last=%d", cfg.eventStream, *override)
		return position{timestamp: *override, streamSeq: lastSeq}, nil
	}

	progress, found, err := readProgress(js, cfg)
	if err != nil {
		return position{}, err
	}

	switch {
	case lastSeq == 0 && !found:
		log.Printf("stream %q is new and no progress exists; publishing from the beginning", cfg.eventStream)
		return position{}, nil

	case lastSeq == 0:
		return position{}, fmt.Errorf(
			"%w: stream %q has never held events, but %q records progress up to timestamp %d: the stream was "+
				"probably deleted and recreated, so its earlier events are missing. Set --timestamp-last=0 "+
				"to republish everything into it, or --timestamp-last=<timestamp> to start later",
			errCannotResume,
			cfg.eventStream,
			cfg.progressBucket,
			progress.Timestamp,
		)

	case !found || progress.StreamSeq != lastSeq:
		return position{}, fmt.Errorf(
			"%w: stream %q holds no events (retention removed them) and ends at sequence %d, but %q has no "+
				"checkpoint for that sequence, so the events that followed it are unknown. Set --timestamp-last "+
				"to the timestamp of the last event consumers received",
			errCannotResume,
			cfg.eventStream,
			lastSeq,
			cfg.progressBucket,
		)

	default:
		log.Printf(
			"stream %q holds no events (retention removed them); resuming after its checkpoint: timestamp=%d stream_seq=%d",
			cfg.eventStream,
			progress.Timestamp,
			lastSeq,
		)
		return position{timestamp: progress.Timestamp, streamSeq: lastSeq}, nil
	}
}

// readTail returns the stream's last sequence and, if the stream holds events, the timestamp of the
// event at that sequence.
func readTail(js nats.JetStreamContext, cfg config) (lastSeq uint64, timestamp uint64, hasEvents bool, err error) {
	for attempt := 0; ; attempt++ {
		info, err := js.StreamInfo(cfg.eventStream)
		if err != nil {
			return 0, 0, false, fmt.Errorf("read stream %q state: %w", cfg.eventStream, err)
		}
		lastSeq = info.State.LastSeq
		if info.State.Msgs == 0 {
			return lastSeq, 0, false, nil
		}

		timestamp, err = lastEventTimestamp(js, cfg, lastSeq)
		if !errors.Is(err, errTailMissing) {
			return lastSeq, timestamp, err == nil, err
		}

		// Retention may have removed the last event since StreamInfo, so read the state again. If
		// events remain but the last one is still missing, it was deleted, and only an operator can
		// tell which events consumers still need.
		if attempt > 0 {
			return 0, 0, false, fmt.Errorf(
				"%w: the message at stream %q's last sequence %d was deleted while earlier events remain, so the "+
					"publisher can't tell where to resume; restore it, or purge the stream and set --timestamp-last",
				errCannotResume,
				cfg.eventStream,
				lastSeq,
			)
		}
	}
}

// lastEventTimestamp returns the TigerBeetle timestamp of the event at the stream's last sequence,
// from its Nats-Msg-Id. Reading by sequence, not subject, keeps the timestamp and sequence of the
// resume position paired, and works whatever subjects older events were published on.
func lastEventTimestamp(js nats.JetStreamContext, cfg config, lastSeq uint64) (uint64, error) {
	msg, err := js.GetMsg(cfg.eventStream, lastSeq)
	if errors.Is(err, nats.ErrMsgNotFound) {
		return 0, fmt.Errorf("%w: stream %q sequence %d", errTailMissing, cfg.eventStream, lastSeq)
	}
	if err != nil {
		return 0, fmt.Errorf("read the last event in stream %q: %w", cfg.eventStream, err)
	}

	msgID := msg.Header.Get(nats.MsgIdHdr)
	cluster, timestamp, ok := parseEventMsgID(msgID)
	if !ok || cluster != cfg.clusterIDDecimal {
		return 0, fmt.Errorf(
			"%w: stream %q ends with message seq=%d (Nats-Msg-Id %q) that this publisher did not write for cluster %s; "+
				"each TigerBeetle cluster needs a stream of its own that nothing else publishes to",
			errCannotResume,
			cfg.eventStream,
			lastSeq,
			msgID,
			cfg.clusterIDDecimal,
		)
	}
	return timestamp, nil
}

// eventMsgID is the Nats-Msg-Id of an event: "<cluster>/<timestamp>".
func eventMsgID(clusterIDDecimal string, timestamp uint64) string {
	return clusterIDDecimal + "/" + strconv.FormatUint(timestamp, 10)
}

func parseEventMsgID(msgID string) (cluster string, timestamp uint64, ok bool) {
	cluster, rawTimestamp, found := strings.Cut(msgID, "/")
	if !found {
		return "", 0, false
	}
	timestamp, err := strconv.ParseUint(rawTimestamp, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return cluster, timestamp, true
}

// readProgress reads the progress checkpoint from the KV bucket's stream leader. KV Get may be
// served by a replica that hasn't caught up.
func readProgress(js nats.JetStreamContext, cfg config) (progressRecord, bool, error) {
	msg, err := js.GetLastMsg(kvStreamName(cfg.progressBucket), "$KV."+cfg.progressBucket+"."+cfg.progressKey())
	if errors.Is(err, nats.ErrMsgNotFound) {
		return progressRecord{}, false, nil
	}
	if err != nil {
		return progressRecord{}, false, fmt.Errorf("read progress %q: %w", cfg.progressKey(), err)
	}

	// A delete or purge leaves a marker message with an empty body.
	if operation := msg.Header.Get("KV-Operation"); operation != "" {
		return progressRecord{}, false, nil
	}

	var stored struct {
		Timestamp *uint64 `json:"timestamp"`
		StreamSeq uint64  `json:"stream_seq"`
		Version   string  `json:"version"`
	}
	if err := json.Unmarshal(msg.Data, &stored); err != nil || stored.Timestamp == nil {
		return progressRecord{}, false, fmt.Errorf("%w: invalid progress checkpoint in %q: %q", errCannotResume, cfg.progressKey(), msg.Data)
	}
	return progressRecord{Timestamp: *stored.Timestamp, StreamSeq: stored.StreamSeq, Version: stored.Version}, true, nil
}

// writeProgress records the last published event's timestamp and stream sequence in the progress
// KV bucket.
func writeProgress(kv nats.KeyValue, cfg config, timestamp uint64, streamSeq uint64) error {
	payload, err := json.Marshal(progressRecord{Timestamp: timestamp, StreamSeq: streamSeq, Version: cfg.version})
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}

	if _, err := kv.Put(cfg.progressKey(), payload); err != nil {
		return fmt.Errorf("write progress key %q: %w", cfg.progressKey(), err)
	}
	return nil
}
