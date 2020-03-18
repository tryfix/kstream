package data

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRecord_RecordKey(t *testing.T) {
	rec := Record{
		Key:   []byte(`k`),
		Value: []byte(`v`),
	}
	if !reflect.DeepEqual(rec.RecordKey(), []byte(`k`)) {
		t.Fail()
	}
}

func TestRecord_RecordValue(t *testing.T) {
	rec := Record{
		Key:   []byte(`k`),
		Value: []byte(`v`),
	}
	if !reflect.DeepEqual(rec.RecordValue(), []byte(`v`)) {
		t.Fail()
	}
}

func TestRecord_String(t *testing.T) {
	r := Record{
		Key:       []byte(`k`),
		Value:     []byte(`v`),
		Offset:    1000,
		Topic:     `test`,
		Partition: 1,
	}
	if r.String() != fmt.Sprintf(`%s_%d_%d`, r.Topic, r.Partition, r.Offset) {
		t.Fail()
	}
}
