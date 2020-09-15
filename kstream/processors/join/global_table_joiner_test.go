package join

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/store"
	"reflect"
	"testing"
)

type rightRecord struct {
	PrimaryKey int `json:"primary_key"`
	ForeignKey int `json:"foreign_key"`
}

type leftRecord struct {
	PrimaryKey int `json:"primary_key"`
	ForeignKey int `json:"foreign_key"`
}

type joinedRecord struct {
	left  leftRecord
	right rightRecord
}

func (e rightRecord) Decode(data []byte) (interface{}, error) {
	v := rightRecord{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func (rightRecord) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

var testStore = store.NewMockStore(
	`test_store`,
	encoding.IntEncoder{},
	rightRecord{},
	backend.NewMockBackend(`test_backend`, 0))

func makeJoiner(typ Type) *GlobalTableJoiner {
	return &GlobalTableJoiner{
		store: testStore,
		KeyMapper: func(key interface{}, value interface{}) (mappedKey interface{}, err error) {
			v, _ := value.(leftRecord)
			return v.ForeignKey, nil

		},
		ValueMapper: func(left interface{}, right interface{}) (joined interface{}, err error) {
			l, _ := left.(leftRecord)
			r, _ := right.(rightRecord)

			return joinedRecord{
				left:  l,
				right: r,
			}, nil
		},
		Typ: typ,
	}
}

func TestGlobalTableJoiner_Join_Inner(t *testing.T) {

	leftRecord := leftRecord{
		PrimaryKey: 1000,
		ForeignKey: 2000,
	}

	rightRecord := rightRecord{
		PrimaryKey: 1000,
		ForeignKey: 2000,
	}

	err := testStore.Set(context.Background(), 2000, rightRecord, 0)
	if err != nil {
		t.Error(err)
	}

	defer testStore.Delete(context.Background(), 2000)

	joiner := makeJoiner(InnerJoin)

	v, err := joiner.Join(context.Background(), 1000, leftRecord)
	if err != nil {
		t.Error(err)
	}

	if _, ok := v.(joinedRecord); !ok {
		t.Error(`invalid record`)
	}

}

func TestGlobalTableJoiner_Join_Inner_Should_Return_Error_When_Right_Null(t *testing.T) {

	leftRecord := leftRecord{
		PrimaryKey: 1000,
		ForeignKey: 2000,
	}

	joiner := makeJoiner(InnerJoin)

	v, err := joiner.Join(context.Background(), 1000, leftRecord)
	if err == nil {
		t.Error(err)
	}

	//log.Fatal(v)

	if v != nil {
		t.Error(`joined value must null when right lookup failed`)
	}

}

func TestGlobalTableJoiner_Join_Inner_Should_Return_Error_When_Left_Null(t *testing.T) {

	rightRecord := rightRecord{
		PrimaryKey: 1000,
		ForeignKey: 2000,
	}

	err := testStore.Set(context.Background(), 2000, rightRecord, 0)
	if err != nil {
		t.Error(err)
	}

	defer testStore.Delete(context.Background(), 2000)

	joiner := makeJoiner(InnerJoin)

	v, err := joiner.Join(context.Background(), 1000, nil)
	if err == nil {
		t.Error(err)
	}

	if v != nil {
		t.Error(`joined value must null when right lookup failed`)
	}

}

func TestGlobalTableJoiner_Join_Left(t *testing.T) {

	leftRecord := leftRecord{
		PrimaryKey: 1000,
		ForeignKey: 2000,
	}

	joiner := makeJoiner(LeftJoin)

	v, err := joiner.Join(context.Background(), 1000, leftRecord)
	if err != nil {
		t.Error(err)
		return
	}

	if _, ok := v.(joinedRecord); !ok {
		t.Error(fmt.Sprintf(`want [joinedRecord] have [%+v]`, reflect.TypeOf(v)))
	}

}
