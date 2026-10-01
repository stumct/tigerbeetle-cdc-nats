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

// progressRecord is the checkpoint stored in the progress KV bucket after each published batch.
type progressRecord struct {
	Timestamp uint64 `json:"timestamp"`
	Version   string `json:"version"`
}

// position is where publishing resumes: after TigerBeetle timestamp `timestamp`, appending to the
// event stream after sequence `streamSeq`.
type position struct {
	timestamp uint64
	streamSeq uint64
}

// recoverPosition decides where publishing resumes. The event stream is the record of what was
// published: publishing continues after the timestamp of its last event. That holds even if the
// previous run crashed before checkpointing, or if a checkpoint survived a NATS failure that lost
// events. The progress checkpoint is used only when retention has emptied the stream. Reads go to
// the stream leaders, never to possibly stale replicas.
//
// --timestamp-last moves the start forward past events the stream doesn't hold yet. It never moves
// it back, because replaying into the same stream would duplicate and reorder events.
func recoverPosition(js nats.JetStreamContext, cfg config) (position, error) {
	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		return position{}, fmt.Errorf("read stream %q state: %w", cfg.eventStream, err)
	}
	lastSeq := info.State.LastSeq
	override := cfg.timestampLast

	// resume is where the stream (or, failing that, the checkpoint) says publishing got to.
	var resume *position
	var resumeSource string

	if info.State.Msgs > 0 {
		timestamp, err := lastEventTimestamp(js, cfg)
		if err != nil {
			return position{}, err
		}
		resume, resumeSource = &position{timestamp: timestamp, streamSeq: lastSeq}, "the stream's last event"
	} else {
		progress, found, err := readProgress(js, cfg)
		if err != nil {
			return position{}, err
		}

		switch {
		case !found:
		case lastSeq > 0:
			resume = &position{timestamp: progress.Timestamp, streamSeq: lastSeq}
			resumeSource = "the progress checkpoint (retention has removed every event from the stream)"
		case override == nil:
			return position{}, fmt.Errorf(
				"%w: stream %q has never held events, but %q records progress up to timestamp %d: the stream was "+
					"probably deleted and recreated, so its earlier events are missing. Set --timestamp-last=0 "+
					"to republish everything into it, or --timestamp-last=<timestamp> to start later",
				errCannotResume,
				cfg.eventStream,
				cfg.progressBucket,
				progress.Timestamp,
			)
		}
	}

	if override != nil {
		if resume == nil || *override >= resume.timestamp {
			log.Printf("starting after --timestamp-last=%d", *override)
			return position{timestamp: *override, streamSeq: lastSeq}, nil
		}
		log.Printf(
			"ignoring --timestamp-last=%d: stream %q is already past it (timestamp %d)",
			*override,
			cfg.eventStream,
			resume.timestamp,
		)
	}

	if resume != nil {
		log.Printf("resuming after %s: timestamp=%d stream_seq=%d", resumeSource, resume.timestamp, resume.streamSeq)
		return *resume, nil
	}

	if lastSeq > 0 {
		return position{}, fmt.Errorf(
			"%w: stream %q has held events but none remain and %q has no progress checkpoint; "+
				"set --timestamp-last to choose where to resume",
			errCannotResume,
			cfg.eventStream,
			cfg.progressBucket,
		)
	}

	log.Printf("stream %q is new and no progress exists; publishing from the beginning", cfg.eventStream)
	return position{}, nil
}

// lastEventTimestamp returns the TigerBeetle timestamp of the stream's last event, from its
// Nats-Msg-Id.
func lastEventTimestamp(js nats.JetStreamContext, cfg config) (uint64, error) {
	subjects := cfg.eventStreamSubjects()
	msg, err := js.GetLastMsg(cfg.eventStream, subjects[0])
	if err != nil {
		return 0, fmt.Errorf("read last event in stream %q: %w", cfg.eventStream, err)
	}

	msgID := msg.Header.Get(nats.MsgIdHdr)
	cluster, timestamp, ok := parseEventMsgID(msgID)
	if !ok || cluster != cfg.clusterIDDecimal {
		return 0, fmt.Errorf(
			"%w: stream %q ends with message seq=%d (Nats-Msg-Id %q) that this publisher did not write for cluster %s; "+
				"each TigerBeetle cluster needs a stream of its own that nothing else publishes to",
			errCannotResume,
			cfg.eventStream,
			msg.Sequence,
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
		Version   string  `json:"version"`
	}
	if err := json.Unmarshal(msg.Data, &stored); err != nil || stored.Timestamp == nil {
		return progressRecord{}, false, fmt.Errorf("%w: invalid progress checkpoint in %q: %q", errCannotResume, cfg.progressKey(), msg.Data)
	}
	return progressRecord{Timestamp: *stored.Timestamp, Version: stored.Version}, true, nil
}

// writeProgress records the last published timestamp in the progress KV bucket.
func writeProgress(kv nats.KeyValue, cfg config, timestamp uint64) error {
	payload, err := json.Marshal(progressRecord{Timestamp: timestamp, Version: cfg.version})
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}

	if _, err := kv.Put(cfg.progressKey(), payload); err != nil {
		return fmt.Errorf("write progress key %q: %w", cfg.progressKey(), err)
	}
	return nil
}
