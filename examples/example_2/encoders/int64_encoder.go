package encoders

import (
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

	/*byt := make([]byte, 4)
	binary.BigEndian.PutUint32(byt, uint32(i))*/

	return []byte(strconv.FormatInt(i, 10)), nil
}

func (Int64Encoder) Decode(data []byte) (interface{}, error) {
	i, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot decode data`)
	}

	return i, nil
	//return int(binary.BigEndian.Uint32(data)), nil
}
