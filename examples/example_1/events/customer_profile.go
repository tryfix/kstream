package events

import (
	"encoding/json"
	"github.com/google/uuid"
)

type CustomerProfileUpdated struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
	Body struct {
		CustomerID     uuid.UUID `json:"customer_id"`
		CustomerName   string    `json:"customer_name"`
		NIC            string    `json:"nic"`
		ContactDetails struct {
			Phone   string `json:"phone"`
			Email   string `json:"email"`
			Address string `json:"address"`
		} `json:"contact_details"`
		DateOfBirth string `json:"date_of_birth"`
		UpdatedAt   int64  `json:"updated_at"`
	} `json:"body"`
	Timestamp int64 `json:"timestamp"`
}

func (c CustomerProfileUpdated) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (c CustomerProfileUpdated) Decode(data []byte) (interface{}, error) {
	cp := CustomerProfileUpdated{}
	err := json.Unmarshal(data, &cp)
	if err != nil {
		return nil, err
	}
	return cp, nil
}
