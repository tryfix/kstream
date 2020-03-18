package events

import "encoding/json"

type AccountCredited struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Body struct {
		AccountNo     int64   `json:"account_no"`
		TransactionId int64   `json:"transaction_id"`
		Amount        float64 `json:"amount"`
		Reason        string  `json:"reason"`
		DebitedFrom   int64   `json:"debited_from"`
		CreditedAt    int64   `json:"credited_at"`
		Location      string  `json:"location"`
	} `json:"body"`
	Timestamp int64 `json:"timestamp"`
}

func (a AccountCredited) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil{
		return nil,err
	}

	return b, nil
}

func (a AccountCredited) Decode(data []byte) (interface{}, error) {
	ac := AccountCredited{}
	err := json.Unmarshal(data, &ac)
	if err != nil{
		return nil,err
	}
	return ac, nil
}
