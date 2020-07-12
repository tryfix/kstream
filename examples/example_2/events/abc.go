package events

import "encoding/json"

type ABC struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	AAA        string `json:"aaa"`
	BBB        string `json:"bbb"`
	CCC        string `json:"ccc"`
	TimestampA int64  `json:"timestamp_a"`
	TimestampB int64  `json:"timestamp_b"`
	TimestampC int64  `json:"timestamp_c"`
}

func (a ABC) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (a ABC) Decode(data []byte) (interface{}, error) {
	ac := ABC{}
	err := json.Unmarshal(data, &ac)
	if err != nil {
		return nil, err
	}
	return ac, nil
}
