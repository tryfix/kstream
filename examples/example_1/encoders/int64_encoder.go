package encoders

import (
	"github.com/google/uuid"
	"github.com/tryfix/errors"
	"reflect"
	"strconv"
)

type Int64Encoder struct{}

func (Int64Encoder) Encode(v interface{}) ([]byte, error) {

	i, ok := v.(int64)
	if !ok {
		j, k := v.(int)
		if !k {
			return nil, errors.Errorf(`invalid type [%v] expected int64`, reflect.TypeOf(v))
		}
		i = int64(j)
	}

	return []byte(strconv.FormatInt(i, 10)), nil
}

func (Int64Encoder) Decode(data []byte) (interface{}, error) {
	i, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot decode data`)
	}

	return i, nil
}

type UuidEncoder struct{}

func (UuidEncoder) Encode(v interface{}) ([]byte, error) {
	i, ok := v.(uuid.UUID)
	if !ok {
		return nil, errors.Errorf(`invalid type [%v] expected int64`, reflect.TypeOf(v))
	}

	return i.MarshalText()
}

func (UuidEncoder) Decode(data []byte) (interface{}, error) {
	uid := uuid.UUID{}
	err := uid.UnmarshalText(data)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot decode data`)
	}

	return uid, nil
}
