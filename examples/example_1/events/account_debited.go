package events

import "encoding/json"

type AccountDebited struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Body struct {
		AccountNo     int64   `json:"account_no"`
		Amount        float64 `json:"amount"`
		TransactionId int64   `json:"transaction_id"`
		Reason        string  `json:"reason"`
		CreditedTo    int64   `json:"credited_to"`
		DebitedAt     int64   `json:"debited_at"`
		Location      string  `json:"location"`
	} `json:"body"`
	Timestamp int64 `json:"timestamp"`
}

func (ad AccountDebited) Encode(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil{
		return nil,err
	}

	return b, nil
}

func (ad AccountDebited) Decode(data []byte) (interface{}, error) {
	debited := AccountDebited{}
	err := json.Unmarshal(data, &debited)
	if err != nil{
		return nil,err
	}
	return debited, nil
}
