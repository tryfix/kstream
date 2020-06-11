package events

import (
	"encoding/json"
	"github.com/google/uuid"
)

type MessageCreated struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Body struct {
		CustomerID uuid.UUID `json:"customer_id"`
		Text       string    `json:"text"`
		Phone      string    `json:"phone"`
		Email      string    `json:"email"`
		Address    string    `json:"address"`
	} `json:"body"`
	Timestamp int64 `json:"timestamp"`
}

func (m MessageCreated) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (m MessageCreated) Decode(data []byte) (interface{}, error) {
	mc := MessageCreated{}
	err := json.Unmarshal(data, &mc)
	if err != nil {
		return nil, err
	}
	return mc, nil
}
