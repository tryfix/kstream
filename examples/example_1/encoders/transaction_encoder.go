package encoders

import (
	"encoding/json"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/examples/example_1/events"
)

type TransactionEncoder struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Body      interface{} `json:"body"`
	Timestamp int64       `json:"timestamp"`
}

func (t TransactionEncoder) Encode(data interface{}) ([]byte, error) {
	panic("implement me")
}

func (t TransactionEncoder) Decode(data []byte) (interface{}, error) {
	te := TransactionEncoder{}
	err := json.Unmarshal(data, &te)
	if err != nil {
		return nil, err
	}
	switch te.Type {
	case `account_credited`:
		ac := events.AccountCredited{}
		err := json.Unmarshal(data, &ac)
		if err != nil {
			return nil, err
		}
		return ac, nil

	case `account_debited`:
		ad := events.AccountDebited{}
		err := json.Unmarshal(data, &ad)
		if err != nil {
			return nil, err
		}
		return ad, nil

	default:
		return nil, errors.New(fmt.Sprintf(`unexpected type received :- %v`, te.Type))
	}
}
