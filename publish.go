package cdcnats

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// publisher appends change events to the event stream strictly in order.
//
// Every message carries Nats-Expected-Last-Sequence set to its predecessor's stream sequence, and
// JetStream stores it only if the stream ends at that sequence. Publishing always starts from a
// position read back from the stream (an event and the sequence it is stored at) and appends events
// densely in TigerBeetle order, so the event at each stream position is fixed. A message that passes
// its sequence check therefore directly follows the right event, even if it is a late message from an
// abandoned attempt or another instance. If a message is lost or rejected, every message pipelined
// after it is rejected too; the publish fails instead of leaving a gap, a duplicate or a reordering,
// and the caller resumes from the stream (see recoverPosition).
//
// That reasoning needs every writer to be a publisher like this one: a foreign message landing at the
// position a pipelined message expects would let it through. Nats-Expected-Last-Msg-Id would catch
// that, but nats-server 2.14+ treats a failed message-ID check on a replicated stream as a critical
// write error and takes the stream's replicas out of service, so it isn't used. Deployments must
// make the publisher the stream's only writer (see the README).
type publisher struct {
	js  nats.JetStreamContext
	cfg config
	// lastSeq is the stream sequence of the last acknowledged event, or the stream's last sequence
	// when publishing started.
	lastSeq uint64
	// maxInFlight is how many published messages may await acknowledgement at once.
	maxInFlight int
	// onStored, if set, is called with each event's stream sequence and timestamp once JetStream
	// confirms it is stored, including events stored before a later one in the same batch fails.
	onStored func(sequence uint64, timestamp uint64)
}

func newPublisher(js nats.JetStreamContext, cfg config, lastSeq uint64, maxInFlight int) *publisher {
	return &publisher{js: js, cfg: cfg, lastSeq: lastSeq, maxInFlight: maxInFlight}
}

// errUnexpectedAck marks an acknowledgement for a different stream position than the publisher
// expected, for example a duplicate of an event already stored.
var errUnexpectedAck = errors.New("unexpected acknowledgement")

// pendingEvent is a published message awaiting its acknowledgement.
type pendingEvent struct {
	future    nats.PubAckFuture
	timestamp uint64
	subject   string
	// sequence is the stream sequence the message must be stored at.
	sequence uint64
}

// publish appends events in order and returns once all of them are acknowledged. At most
// p.maxInFlight messages are unacknowledged at a time.
//
// If a message fails, publish waits for every message still outstanding to resolve before returning
// the error. The caller then resumes from what the stream holds, and nothing it published earlier is
// still pending in the client.
func (p *publisher) publish(ctx context.Context, events []types.ChangeEvent) error {
	maxInFlight := p.maxInFlight
	inFlight := make([]pendingEvent, 0, min(len(events), maxInFlight))
	nextSeq := p.lastSeq + 1

	for _, event := range events {
		if len(inFlight) == maxInFlight {
			if err := p.await(ctx, inFlight[0]); err != nil {
				return p.settle(ctx, inFlight[1:], err)
			}
			inFlight = inFlight[1:]
		}

		msg, err := buildEventMessage(p.cfg, event)
		if err != nil {
			return p.settle(ctx, inFlight, err)
		}
		msg.Header.Set(nats.ExpectedStreamHdr, p.cfg.eventStream)
		msg.Header.Set(nats.ExpectedLastSeqHdr, strconv.FormatUint(nextSeq-1, 10))

		future, err := p.js.PublishMsgAsync(msg)
		if err != nil {
			err = fmt.Errorf("publish event timestamp=%d subject=%q: %w", event.Timestamp, msg.Subject, err)
			return p.settle(ctx, inFlight, err)
		}

		inFlight = append(inFlight, pendingEvent{
			future:    future,
			timestamp: event.Timestamp,
			subject:   msg.Subject,
			sequence:  nextSeq,
		})
		nextSeq++
	}

	for i, pending := range inFlight {
		if err := p.await(ctx, pending); err != nil {
			return p.settle(ctx, inFlight[i+1:], err)
		}
	}
	return nil
}

// await waits for one message's acknowledgement and checks it was stored where expected. The
// JetStream context's publish timeout guarantees the message resolves.
func (p *publisher) await(ctx context.Context, pending pendingEvent) error {
	var ack *nats.PubAck
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-pending.future.Err():
		if err == nil {
			err = fmt.Errorf("async publish failed without error details")
		}
		if isWrongLastSequence(err) {
			err = fmt.Errorf("stream %q no longer ends at sequence %d, because an earlier message was lost or "+
				"another message was appended: %w", p.cfg.eventStream, pending.sequence-1, err)
		}
		return fmt.Errorf("publish event timestamp=%d subject=%q: %w", pending.timestamp, pending.subject, err)
	case ack = <-pending.future.Ok():
	}

	if ack == nil || ack.Stream != p.cfg.eventStream || ack.Sequence != pending.sequence || ack.Duplicate {
		return fmt.Errorf(
			"%w: publish event timestamp=%d: acknowledged as %+v, expected stream %q sequence %d",
			errUnexpectedAck,
			pending.timestamp,
			ack,
			p.cfg.eventStream,
			pending.sequence,
		)
	}

	p.lastSeq = ack.Sequence
	if p.onStored != nil {
		p.onStored(ack.Sequence, pending.timestamp)
	}
	return nil
}

// settle waits for each outstanding message to be acknowledged, rejected or timed out, then returns
// err. Messages stored meanwhile are still reported to onStored. It stops waiting if ctx is done,
// because the run is ending and won't publish again.
func (p *publisher) settle(ctx context.Context, outstanding []pendingEvent, err error) error {
	for _, pending := range outstanding {
		select {
		case ack := <-pending.future.Ok():
			if ack != nil && ack.Stream == p.cfg.eventStream && !ack.Duplicate && p.onStored != nil {
				p.onStored(ack.Sequence, pending.timestamp)
			}
		case <-pending.future.Err():
		case <-ctx.Done():
			return err
		}
	}
	return err
}

// buildEventMessage encodes a change event as a JSON message on its subject, with routing headers
// and a deterministic Nats-Msg-Id ("<cluster>/<timestamp>") that JetStream uses to drop duplicates.
func buildEventMessage(cfg config, event types.ChangeEvent) (*nats.Msg, error) {
	body, eventType, err := encodeEventJSON(event)
	if err != nil {
		return nil, fmt.Errorf("encode change event timestamp=%d: %w", event.Timestamp, err)
	}

	subject := cfg.subjectForEvent(event.Ledger, eventType)
	msg := nats.NewMsg(subject)
	msg.Data = body
	msg.Header = nats.Header{}
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set("event_type", eventType)
	msg.Header.Set("ledger", strconv.FormatUint(uint64(event.Ledger), 10))
	msg.Header.Set("transfer_code", strconv.FormatUint(uint64(event.TransferCode), 10))
	msg.Header.Set("debit_account_code", strconv.FormatUint(uint64(event.DebitAccountCode), 10))
	msg.Header.Set("credit_account_code", strconv.FormatUint(uint64(event.CreditAccountCode), 10))
	msg.Header.Set(nats.MsgIdHdr, eventMsgID(cfg.clusterIDDecimal, event.Timestamp))

	return msg, nil
}

// JetStream error codes for transient conditions that nats.go doesn't name.
const (
	// jsErrCodeDuplicateMessageInProcess: the message's Nats-Msg-Id matches one still being replicated,
	// for example an earlier attempt whose acknowledgement timed out.
	jsErrCodeDuplicateMessageInProcess nats.ErrorCode = 10158
	// jsErrCodeStreamOffline: no peer of the stream is available, for example during a rolling restart.
	jsErrCodeStreamOffline nats.ErrorCode = 10118
)

// isTransient reports whether a publish or resume failure is one that resuming from the stream can
// get past: a lost or late response, a leader election, a reconnect, or a sequence check failing
// because a message landed unexpectedly. Anything else, such as an encoding error, a sealed stream or
// a message over the stream's size limit, needs an operator, so the run stops instead of retrying.
func isTransient(err error) bool {
	if isWrongLastSequence(err) || errors.Is(err, errUnexpectedAck) || errors.Is(err, errTailMoving) {
		return true
	}

	for _, transient := range []error{
		context.DeadlineExceeded,
		nats.ErrTimeout,
		nats.ErrAsyncPublishTimeout,
		nats.ErrNoResponders,
		nats.ErrNoStreamResponse,
		nats.ErrDisconnected,
		nats.ErrConnectionReconnecting,
		nats.ErrReconnectBufExceeded,
	} {
		if errors.Is(err, transient) {
			return true
		}
	}

	// 503: JetStream is temporarily unavailable, for example while a stream elects a leader.
	// 429: the stream's inbound queue is full.
	var apiErr *nats.APIError
	return errors.As(err, &apiErr) && (apiErr.Code == 503 || apiErr.Code == 429 ||
		apiErr.ErrorCode == jsErrCodeDuplicateMessageInProcess ||
		apiErr.ErrorCode == jsErrCodeStreamOffline)
}
