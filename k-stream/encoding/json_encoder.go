package encoding

import "encoding/json"

type JsonSerializer struct{}

func NewJsonSerDes() *JsonSerializer {
	return &JsonSerializer{}
}

func (s *JsonSerializer) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (s *JsonSerializer) Decode(byt []byte, v interface{}) error {
	return json.Unmarshal(byt, &v)

}
