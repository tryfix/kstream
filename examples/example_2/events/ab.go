package events

import "encoding/json"

type AB struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	AAA        string `json:"aaa"`
	BBB        string `json:"bbb"`
	TimestampA int64  `json:"timestamp_a"`
	TimestampB int64  `json:"timestamp_b"`
}

func (a AB) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (a AB) Decode(data []byte) (interface{}, error) {
	ac := AB{}
	err := json.Unmarshal(data, &ac)
	if err != nil {
		return nil, err
	}
	return ac, nil
}
