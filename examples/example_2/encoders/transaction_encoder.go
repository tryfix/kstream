package encoders

import (
	"encoding/json"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/examples/example_2/events"
)

type CommonEncoder struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Body      interface{} `json:"body"`
	Timestamp int64       `json:"timestamp"`
}

func (t CommonEncoder) Encode(data interface{}) ([]byte, error) {
	panic("implement me")
}

func (t CommonEncoder) Decode(data []byte) (interface{}, error) {
	te := CommonEncoder{}
	err := json.Unmarshal(data, &te)
	if err != nil {
		return nil, err
	}
	switch te.Type {
	case `aa`:
		ac := events.AA{}
		err := json.Unmarshal(data, &ac)
		if err != nil {
			return nil, err
		}
		return ac, nil

	case `bb`:
		ad := events.BB{}
		err := json.Unmarshal(data, &ad)
		if err != nil {
			return nil, err
		}
		return ad, nil

	default:
		return nil, errors.New(fmt.Sprintf(`unexpected type received :- %v`, te.Type))
	}
}
