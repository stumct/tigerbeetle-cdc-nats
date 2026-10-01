package cdcnats

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// publisher appends change events to the event stream strictly in order.
//
// Every message carries Nats-Expected-Last-Sequence set to the stream sequence of the event before
// it, so JetStream stores a message only if it directly follows its predecessor. If a message is
// lost or rejected, every message pipelined after it is rejected too, and a write by anything else
// breaks the chain. Each of these fails the publish instead of leaving a gap, a duplicate or a
// reordering in the stream. Restarting resumes from the stream's last event (see recoverPosition).
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

	for _, event := range events {
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
		if isWrongLastSequence(err) {
			err = fmt.Errorf("stream %q no longer ends at sequence %d, because an earlier message was lost or another writer appended to it: %w",
				p.cfg.eventStream, pending.sequence-1, err)
		}
		return fmt.Errorf("publish event timestamp=%d subject=%q: %w", pending.timestamp, pending.subject, err)
	case ack = <-pending.future.Ok():
	}

	if ack == nil || ack.Stream != p.cfg.eventStream || ack.Sequence != pending.sequence || ack.Duplicate {
		return fmt.Errorf(
			"publish event timestamp=%d: acknowledged as %+v, expected stream %q sequence %d",
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
