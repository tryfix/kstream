package encoding

import (
	"github.com/tryfix/errors"
	"reflect"
	"strconv"
)

type IntEncoder struct{}

func (IntEncoder) Encode(v interface{}) ([]byte, error) {

	i, ok := v.(int)
	if !ok {
		return nil, errors.Errorf(`invalid type [%v] expected int`, reflect.TypeOf(v))
	}

	return []byte(strconv.Itoa(i)), nil
}

func (IntEncoder) Decode(data []byte) (interface{}, error) {
	i, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot decode data`)
	}

	return i, nil
}
