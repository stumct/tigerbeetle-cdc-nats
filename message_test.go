package cdcnats

import (
	"math"
	"testing"

	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func TestEncodeEventJSON_ZeroCase(t *testing.T) {
	t.Parallel()

	body, eventType, err := encodeEventJSON(types.ChangeEvent{})
	if err != nil {
		t.Fatalf("encodeEventJSON() error = %v", err)
	}

	if eventType != "single_phase" {
		t.Fatalf("event type = %q, want %q", eventType, "single_phase")
	}

	const expected = `{"timestamp":0,"type":"single_phase","ledger":0,"transfer":{"id":0,"amount":0,"pending_id":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"timeout":0,"code":0,"flags":0,"timestamp":0},"debit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0},"credit_account":{"id":0,"debits_pending":0,"debits_posted":0,"credits_pending":0,"credits_posted":0,"user_data_128":0,"user_data_64":0,"user_data_32":0,"code":0,"flags":0,"timestamp":0}}`

	if got := string(body); got != expected {
		t.Fatalf("body mismatch\n got: %s\nwant: %s", got, expected)
	}

	if got, want := len(body), 564; got != want {
		t.Fatalf("body length = %d, want %d", got, want)
	}
}

func TestEncodeEventJSON_WorstCase(t *testing.T) {
	t.Parallel()

	max128 := types.BytesToUint128([16]byte{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	})

	event := types.ChangeEvent{
		TransferID:                  max128,
		TransferAmount:              max128,
		TransferPendingID:           max128,
		TransferUserData128:         max128,
		TransferUserData64:          math.MaxUint64,
		TransferUserData32:          math.MaxUint32,
		TransferTimeout:             math.MaxUint32,
		TransferCode:                math.MaxUint16,
		TransferFlags:               math.MaxUint16,
		Ledger:                      math.MaxUint32,
		Type:                        types.ChangeEventTwoPhasePending,
		DebitAccountID:              max128,
		DebitAccountDebitsPending:   max128,
		DebitAccountDebitsPosted:    max128,
		DebitAccountCreditsPending:  max128,
		DebitAccountCreditsPosted:   max128,
		DebitAccountUserData128:     max128,
		DebitAccountUserData64:      math.MaxUint64,
		DebitAccountUserData32:      math.MaxUint32,
		DebitAccountCode:            math.MaxUint16,
		DebitAccountFlags:           math.MaxUint16,
		CreditAccountID:             max128,
		CreditAccountDebitsPending:  max128,
		CreditAccountDebitsPosted:   max128,
		CreditAccountCreditsPending: max128,
		CreditAccountCreditsPosted:  max128,
		CreditAccountUserData128:    max128,
		CreditAccountUserData64:     math.MaxUint64,
		CreditAccountUserData32:     math.MaxUint32,
		CreditAccountCode:           math.MaxUint16,
		CreditAccountFlags:          math.MaxUint16,
		Timestamp:                   math.MaxUint64,
		TransferTimestamp:           math.MaxUint64,
		DebitAccountTimestamp:       math.MaxUint64,
		CreditAccountTimestamp:      math.MaxUint64,
	}

	body, eventType, err := encodeEventJSON(event)
	if err != nil {
		t.Fatalf("encodeEventJSON() error = %v", err)
	}

	if eventType != "two_phase_pending" {
		t.Fatalf("event type = %q, want %q", eventType, "two_phase_pending")
	}

	const expected = `{"timestamp":"18446744073709551615","type":"two_phase_pending","ledger":4294967295,"transfer":{"id":"340282366920938463463374607431768211455","amount":"340282366920938463463374607431768211455","pending_id":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"timeout":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"debit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"},"credit_account":{"id":"340282366920938463463374607431768211455","debits_pending":"340282366920938463463374607431768211455","debits_posted":"340282366920938463463374607431768211455","credits_pending":"340282366920938463463374607431768211455","credits_posted":"340282366920938463463374607431768211455","user_data_128":"340282366920938463463374607431768211455","user_data_64":"18446744073709551615","user_data_32":4294967295,"code":65535,"flags":65535,"timestamp":"18446744073709551615"}}`

	if got := string(body); got != expected {
		t.Fatalf("body mismatch\n got: %s\nwant: %s", got, expected)
	}

	if got, want := len(body), 1425; got != want {
		t.Fatalf("body length = %d, want %d", got, want)
	}
}
