package events

import "encoding/json"

type CC struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	CCC       string `json:"ccc"`
	Timestamp int64  `json:"timestamp"`
}

func (ad CC) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (ad CC) Decode(data []byte) (interface{}, error) {
	debited := CC{}
	err := json.Unmarshal(data, &debited)
	if err != nil {
		return nil, err
	}
	return debited, nil
}
