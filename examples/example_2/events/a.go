package events

import "encoding/json"

type AA struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	AAA       string `json:"aaa"`
	Timestamp int64  `json:"timestamp"`
}

func (a AA) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (a AA) Decode(data []byte) (interface{}, error) {
	ac := AA{}
	err := json.Unmarshal(data, &ac)
	if err != nil {
		return nil, err
	}
	return ac, nil
}
