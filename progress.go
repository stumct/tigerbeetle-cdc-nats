package cdcnats

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// errCannotResume marks conditions where the publisher can't tell where to resume, or resuming would
// corrupt the stream. Retrying won't help: an operator has to decide.
var errCannotResume = errors.New("cannot resume publishing")

var (
	// errTailMissing reports that the message at the stream's last sequence no longer exists.
	errTailMissing = errors.New("the stream's last message no longer exists")
	// errTailMoving reports that the stream's last message kept disappearing as it was read, because
	// messages were appended while retention removed old ones. Retrying later will settle.
	errTailMoving = errors.New("the stream's last message kept changing while it was read")
)

// maxTailReads bounds how many times readTail reads the stream state while its last message keeps
// disappearing.
const maxTailReads = 5

// progressRecord is the checkpoint stored in the progress KV bucket after each published batch:
// the last published event's timestamp, the stream sequence it was stored at, and when that stream
// was created, which tells a checkpoint for a deleted and recreated stream of the same name apart.
type progressRecord struct {
	Timestamp     uint64    `json:"timestamp"`
	StreamSeq     uint64    `json:"stream_seq"`
	StreamCreated time.Time `json:"stream_created"`
	Version       string    `json:"version"`
}

// position is where publishing resumes: after TigerBeetle timestamp `timestamp`, appending to the
// event stream after sequence `streamSeq`.
type position struct {
	timestamp uint64
	streamSeq uint64
	// streamCreated identifies the stream incarnation the position belongs to; checkpoints record it.
	streamCreated time.Time
	// storedTimestamp is the timestamp of the last event known to be stored (the stream's last event,
	// or the checkpoint's), or 0 if unknown. It differs from timestamp after --timestamp-last.
	storedTimestamp uint64
}

// recoverPosition decides where publishing resumes. Reads go to the stream leaders, never to
// possibly stale replicas.
//
// The event stream is the record of what was published: publishing continues after the stream's
// last event, the message at its last sequence. That holds even if the previous run crashed before
// checkpointing, or a checkpoint outlived events NATS lost. If retention has removed every event,
// the progress checkpoint is used, but only if it was written for the stream's last sequence.
//
// Either way, the stream's sequences map to TigerBeetle events in order, with none skipped, and every
// copy of the publisher resumes on that same mapping. publisher relies on this. So --timestamp-last
// applies only when there is no verified position to continue from: a new or recreated stream, or an
// emptied one whose checkpoint doesn't match. Skipping ahead in a stream that already has a position
// would change the mapping under messages an earlier instance may still have in flight, so it needs
// a new stream. This also makes the flag safe to leave set.
func recoverPosition(js nats.JetStreamContext, cfg config) (position, error) {
	tail, err := readTail(js, cfg)
	if err != nil {
		return position{}, err
	}
	lastSeq, created := tail.lastSeq, tail.created
	override := cfg.timestampLast

	if tail.hasEvents {
		if override != nil {
			log.Printf("ignoring --timestamp-last=%d: stream %q already holds events", *override, cfg.eventStream)
		}
		log.Printf("resuming after the stream's last event: timestamp=%d stream_seq=%d", tail.timestamp, lastSeq)
		return position{timestamp: tail.timestamp, streamSeq: lastSeq, streamCreated: created, storedTimestamp: tail.timestamp}, nil
	}

	progress, found, err := readProgress(js, cfg)
	if err != nil {
		return position{}, err
	}

	if lastSeq > 0 && found && progress.StreamSeq == lastSeq && progress.StreamCreated.Equal(created) {
		if override != nil {
			log.Printf("ignoring --timestamp-last=%d: stream %q has a checkpoint for its last sequence", *override, cfg.eventStream)
		}
		log.Printf(
			"stream %q holds no events (retention removed them); resuming after its checkpoint: timestamp=%d stream_seq=%d",
			cfg.eventStream,
			progress.Timestamp,
			lastSeq,
		)
		return position{timestamp: progress.Timestamp, streamSeq: lastSeq, streamCreated: created, storedTimestamp: progress.Timestamp}, nil
	}

	if override != nil {
		log.Printf("stream %q has no position to continue from; starting after --timestamp-last=%d", cfg.eventStream, *override)
		return position{timestamp: *override, streamSeq: lastSeq, streamCreated: created}, nil
	}

	switch {
	case lastSeq == 0 && !found:
		log.Printf("stream %q is new and no progress exists; publishing from the beginning", cfg.eventStream)
		return position{streamCreated: created}, nil

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

	default:
		return position{}, fmt.Errorf(
			"%w: stream %q holds no events (retention removed them) and ends at sequence %d, but %q has no "+
				"checkpoint for that sequence of this stream, so the events that followed it are unknown. Set "+
				"--timestamp-last to the timestamp of the last event consumers received",
			errCannotResume,
			cfg.eventStream,
			lastSeq,
			cfg.progressBucket,
		)
	}
}

// streamTail is the state of the event stream that publishing resumes from.
type streamTail struct {
	lastSeq uint64
	created time.Time
	// hasEvents is set when the stream holds events; timestamp is then the last event's.
	hasEvents bool
	timestamp uint64
}

// readTail reads the stream's state and, if it holds events, the timestamp of the event at its last
// sequence.
func readTail(js nats.JetStreamContext, cfg config) (streamTail, error) {
	var previousLastSeq uint64
	for attempt := range maxTailReads {
		info, err := js.StreamInfo(cfg.eventStream)
		if err != nil {
			return streamTail{}, fmt.Errorf("read stream %q state: %w", cfg.eventStream, err)
		}
		tail := streamTail{lastSeq: info.State.LastSeq, created: info.Created}
		if info.State.Msgs == 0 {
			return tail, nil
		}

		timestamp, err := lastEventTimestamp(js, cfg, tail.lastSeq)
		if !errors.Is(err, errTailMissing) {
			if err != nil {
				return streamTail{}, err
			}
			tail.hasEvents, tail.timestamp = true, timestamp
			return tail, nil
		}

		// The last message vanished after StreamInfo. Retention removes the oldest messages first, so
		// if it removed this one, the stream is now empty or has grown. If neither happened, it was
		// deleted while earlier events remain, and only an operator can tell which events consumers
		// still need.
		if attempt > 0 && tail.lastSeq == previousLastSeq {
			return streamTail{}, fmt.Errorf(
				"%w: the message at stream %q's last sequence %d was deleted while earlier events remain, so the "+
					"publisher can't tell where to resume; restore it, or delete and recreate the stream and set "+
					"--timestamp-last",
				errCannotResume,
				cfg.eventStream,
				tail.lastSeq,
			)
		}
		previousLastSeq = tail.lastSeq
	}
	return streamTail{}, fmt.Errorf("%w: stream %q", errTailMoving, cfg.eventStream)
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
		Timestamp     *uint64   `json:"timestamp"`
		StreamSeq     uint64    `json:"stream_seq"`
		StreamCreated time.Time `json:"stream_created"`
		Version       string    `json:"version"`
	}
	if err := json.Unmarshal(msg.Data, &stored); err != nil || stored.Timestamp == nil {
		return progressRecord{}, false, fmt.Errorf("%w: invalid progress checkpoint in %q: %q", errCannotResume, cfg.progressKey(), msg.Data)
	}
	return progressRecord{
		Timestamp:     *stored.Timestamp,
		StreamSeq:     stored.StreamSeq,
		StreamCreated: stored.StreamCreated,
		Version:       stored.Version,
	}, true, nil
}

// writeProgress records the last published event's timestamp and stream sequence, and the stream's
// creation time, in the progress KV bucket.
func writeProgress(kv nats.KeyValue, cfg config, timestamp uint64, streamSeq uint64, streamCreated time.Time) error {
	payload, err := json.Marshal(progressRecord{
		Timestamp:     timestamp,
		StreamSeq:     streamSeq,
		StreamCreated: streamCreated,
		Version:       cfg.version,
	})
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}

	if _, err := kv.Put(cfg.progressKey(), payload); err != nil {
		return fmt.Errorf("write progress key %q: %w", cfg.progressKey(), err)
	}
	return nil
}
