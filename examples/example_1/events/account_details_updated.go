package events

import "encoding/json"

type AccountDetailsUpdated struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Body struct {
		AccountNo   int64    `json:"account_no"`
		AccountType string `json:"account_type"`
		CustomerID  int64    `json:"customer_id"`
		Branch      string `json:"branch"`
		BranchCode  int    `json:"branch_code"`
		UpdatedAt   int64   `json:"updated_at"`
	} `json:"body"`
	Timestamp int64 `json:"timestamp"`
}

func (a AccountDetailsUpdated) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil{
		return nil,err
	}

	return b, nil
}

func (a AccountDetailsUpdated) Decode(data []byte) (interface{}, error) {
	ad := AccountDetailsUpdated{}
	err := json.Unmarshal(data, &ad)
	if err != nil{
		return nil,err
	}
	return ad, nil
}

