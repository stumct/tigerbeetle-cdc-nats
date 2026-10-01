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
// Every message names its predecessor, and JetStream stores it only if the stream ends with exactly
// that message. Nats-Expected-Last-Sequence requires the predecessor's position. Within a batch,
// Nats-Expected-Last-Msg-Id also requires its identity, so a message that lands where the
// predecessor should be (from another writer, or one of ours that timed out earlier) can't let a
// later message through. The first message of a batch follows an event already confirmed by its
// acknowledgement or by reading it back, so its position identifies it.
//
// If a message is lost or rejected, every message pipelined after it is rejected too. The publish
// fails instead of leaving a gap, a duplicate or a reordering, and the caller resumes from the
// stream's last event (see recoverPosition).
type publisher struct {
	js  nats.JetStreamContext
	cfg config
	// lastSeq is the stream sequence of the last acknowledged event, or the stream's last sequence
	// when publishing started.
	lastSeq uint64
}

func newPublisher(js nats.JetStreamContext, cfg config, lastSeq uint64) *publisher {
	return &publisher{js: js, cfg: cfg, lastSeq: lastSeq}
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
// cfg.maxInFlight() messages are unacknowledged at a time.
//
// If a message fails, publish waits for every message still outstanding to resolve before returning
// the error. The caller then resumes from what the stream holds, and nothing it published earlier is
// still pending in the client.
func (p *publisher) publish(ctx context.Context, events []types.ChangeEvent) error {
	maxInFlight := p.cfg.maxInFlight()
	inFlight := make([]pendingEvent, 0, min(len(events), maxInFlight))
	nextSeq := p.lastSeq + 1

	for i, event := range events {
		if len(inFlight) == maxInFlight {
			if err := p.await(ctx, inFlight[0]); err != nil {
				return settle(ctx, inFlight[1:], err)
			}
			inFlight = inFlight[1:]
		}

		msg, err := buildEventMessage(p.cfg, event)
		if err != nil {
			return settle(ctx, inFlight, err)
		}
		msg.Header.Set(nats.ExpectedStreamHdr, p.cfg.eventStream)
		msg.Header.Set(nats.ExpectedLastSeqHdr, strconv.FormatUint(nextSeq-1, 10))
		if i > 0 {
			msg.Header.Set(nats.ExpectedLastMsgIdHdr, eventMsgID(p.cfg.clusterIDDecimal, events[i-1].Timestamp))
		}

		future, err := p.js.PublishMsgAsync(msg)
		if err != nil {
			err = fmt.Errorf("publish event timestamp=%d subject=%q: %w", event.Timestamp, msg.Subject, err)
			return settle(ctx, inFlight, err)
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
			return settle(ctx, inFlight[i+1:], err)
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
		if isFenceRejection(err) {
			err = fmt.Errorf("stream %q no longer ends with the event before this one at sequence %d, because an "+
				"earlier message was lost or another message was appended: %w", p.cfg.eventStream, pending.sequence-1, err)
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
	return nil
}

// settle waits for each outstanding message to be acknowledged, rejected or timed out, then returns
// err. It stops waiting if ctx is done, because the run is ending and won't publish again.
func settle(ctx context.Context, outstanding []pendingEvent, err error) error {
	for _, pending := range outstanding {
		select {
		case <-pending.future.Ok():
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

// jsErrCodeStreamWrongLastMsgID is JetStream's error code for a failed Nats-Expected-Last-Msg-Id
// check. nats.go doesn't name it.
const jsErrCodeStreamWrongLastMsgID nats.ErrorCode = 10070

// isFenceRejection reports whether JetStream rejected a message because the stream doesn't end with
// the predecessor the message named.
func isFenceRejection(err error) bool {
	var apiErr *nats.APIError
	return isWrongLastSequence(err) || (errors.As(err, &apiErr) && apiErr.ErrorCode == jsErrCodeStreamWrongLastMsgID)
}

// isTransient reports whether a publish or resume failure is one that resuming from the stream can
// get past: a lost or late response, a leader election, a reconnect, or a fence rejection caused by
// a message that landed unexpectedly. Anything else, such as an encoding error, a sealed stream or a
// message over the stream's size limit, needs an operator, so the run stops instead of retrying.
func isTransient(err error) bool {
	if isFenceRejection(err) || errors.Is(err, errUnexpectedAck) {
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
	} {
		if errors.Is(err, transient) {
			return true
		}
	}

	// 503: JetStream is temporarily unavailable, for example while a stream elects a leader.
	var apiErr *nats.APIError
	return errors.As(err, &apiErr) && apiErr.Code == 503
}
