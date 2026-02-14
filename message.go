package cdcnats

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

const maxPortableJSONInteger uint64 = 9007199254740991 // 2^53 - 1

var maxPortableJSONBigInt = big.NewInt(int64(maxPortableJSONInteger))

type portableUint64 uint64

func (value portableUint64) MarshalJSON() ([]byte, error) {
	v := uint64(value)
	if v > maxPortableJSONInteger {
		return json.Marshal(strconv.FormatUint(v, 10))
	}
	return strconv.AppendUint(nil, v, 10), nil
}

type portableUint128 struct {
	value types.Uint128
}

func newPortableUint128(value types.Uint128) portableUint128 {
	return portableUint128{value: value}
}

func (value portableUint128) MarshalJSON() ([]byte, error) {
	bigValue := value.value.BigInt()
	text := bigValue.String()
	if bigValue.Cmp(maxPortableJSONBigInt) > 0 {
		return json.Marshal(text)
	}
	return []byte(text), nil
}

type transferMessage struct {
	ID          portableUint128 `json:"id"`
	Amount      portableUint128 `json:"amount"`
	PendingID   portableUint128 `json:"pending_id"`
	UserData128 portableUint128 `json:"user_data_128"`
	UserData64  portableUint64  `json:"user_data_64"`
	UserData32  uint32          `json:"user_data_32"`
	Timeout     uint32          `json:"timeout"`
	Code        uint16          `json:"code"`
	Flags       uint16          `json:"flags"`
	Timestamp   portableUint64  `json:"timestamp"`
}

type accountMessage struct {
	ID             portableUint128 `json:"id"`
	DebitsPending  portableUint128 `json:"debits_pending"`
	DebitsPosted   portableUint128 `json:"debits_posted"`
	CreditsPending portableUint128 `json:"credits_pending"`
	CreditsPosted  portableUint128 `json:"credits_posted"`
	UserData128    portableUint128 `json:"user_data_128"`
	UserData64     portableUint64  `json:"user_data_64"`
	UserData32     uint32          `json:"user_data_32"`
	Code           uint16          `json:"code"`
	Flags          uint16          `json:"flags"`
	Timestamp      portableUint64  `json:"timestamp"`
}

type cdcMessage struct {
	Timestamp     portableUint64  `json:"timestamp"`
	Type          string          `json:"type"`
	Ledger        uint32          `json:"ledger"`
	Transfer      transferMessage `json:"transfer"`
	DebitAccount  accountMessage  `json:"debit_account"`
	CreditAccount accountMessage  `json:"credit_account"`
}

func encodeEventType(eventType types.ChangeEventType) (string, error) {
	switch eventType {
	case types.ChangeEventSinglePhase:
		return "single_phase", nil
	case types.ChangeEventTwoPhasePending:
		return "two_phase_pending", nil
	case types.ChangeEventTwoPhasePosted:
		return "two_phase_posted", nil
	case types.ChangeEventTwoPhaseVoided:
		return "two_phase_voided", nil
	case types.ChangeEventTwoPhaseExpired:
		return "two_phase_expired", nil
	default:
		return "", fmt.Errorf("unsupported change event type: %d", eventType)
	}
}

func encodeEventJSON(event types.ChangeEvent) ([]byte, string, error) {
	eventType, err := encodeEventType(event.Type)
	if err != nil {
		return nil, "", err
	}

	message := cdcMessage{
		Timestamp: portableUint64(event.Timestamp),
		Type:      eventType,
		Ledger:    event.Ledger,
		Transfer: transferMessage{
			ID:          newPortableUint128(event.TransferID),
			Amount:      newPortableUint128(event.TransferAmount),
			PendingID:   newPortableUint128(event.TransferPendingID),
			UserData128: newPortableUint128(event.TransferUserData128),
			UserData64:  portableUint64(event.TransferUserData64),
			UserData32:  event.TransferUserData32,
			Timeout:     event.TransferTimeout,
			Code:        event.TransferCode,
			Flags:       event.TransferFlags,
			Timestamp:   portableUint64(event.TransferTimestamp),
		},
		DebitAccount: accountMessage{
			ID:             newPortableUint128(event.DebitAccountID),
			DebitsPending:  newPortableUint128(event.DebitAccountDebitsPending),
			DebitsPosted:   newPortableUint128(event.DebitAccountDebitsPosted),
			CreditsPending: newPortableUint128(event.DebitAccountCreditsPending),
			CreditsPosted:  newPortableUint128(event.DebitAccountCreditsPosted),
			UserData128:    newPortableUint128(event.DebitAccountUserData128),
			UserData64:     portableUint64(event.DebitAccountUserData64),
			UserData32:     event.DebitAccountUserData32,
			Code:           event.DebitAccountCode,
			Flags:          event.DebitAccountFlags,
			Timestamp:      portableUint64(event.DebitAccountTimestamp),
		},
		CreditAccount: accountMessage{
			ID:             newPortableUint128(event.CreditAccountID),
			DebitsPending:  newPortableUint128(event.CreditAccountDebitsPending),
			DebitsPosted:   newPortableUint128(event.CreditAccountDebitsPosted),
			CreditsPending: newPortableUint128(event.CreditAccountCreditsPending),
			CreditsPosted:  newPortableUint128(event.CreditAccountCreditsPosted),
			UserData128:    newPortableUint128(event.CreditAccountUserData128),
			UserData64:     portableUint64(event.CreditAccountUserData64),
			UserData32:     event.CreditAccountUserData32,
			Code:           event.CreditAccountCode,
			Flags:          event.CreditAccountFlags,
			Timestamp:      portableUint64(event.CreditAccountTimestamp),
		},
	}

	body, err := json.Marshal(message)
	if err != nil {
		return nil, "", fmt.Errorf("marshal change event: %w", err)
	}

	return body, eventType, nil
}
