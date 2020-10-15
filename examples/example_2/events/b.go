package events

import "encoding/json"

type BB struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	BBB       string `json:"bbb"`
	Timestamp int64  `json:"timestamp"`
}

func (ad BB) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (ad BB) Decode(data []byte) (interface{}, error) {
	debited := BB{}
	err := json.Unmarshal(data, &debited)
	if err != nil {
		return nil, err
	}
	return debited, nil
}
