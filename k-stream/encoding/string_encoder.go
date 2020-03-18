package encoding

import (
	"github.com/tryfix/errors"
	"reflect"
)

type StringEncoder struct{}

func (s StringEncoder) Encode(v interface{}) ([]byte, error) {
	str, ok := v.(string)
	if !ok {
		return nil, errors.Errorf(`invalid type [%+v] expected string`, reflect.TypeOf(v))
	}

	return []byte(str), nil
}

func (s StringEncoder) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}
