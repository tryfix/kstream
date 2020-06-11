package store

import (
	"github.com/google/uuid"
	"reflect"
	"strings"
	"testing"
)

func TestNewIndex(t *testing.T) {
	var mapper func(key, val interface{}) (idx string)
	index := NewStringHashIndex(`foo`, mapper)
	type args struct {
		name   string
		mapper KeyMapper
	}
	tests := []struct {
		name string
		args args
		want Index
	}{
		{name: `new`, args: struct {
			name   string
			mapper KeyMapper
		}{name: `foo`, mapper: mapper}, want: index},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewStringHashIndex(tt.args.name, tt.args.mapper); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStringHashIndex() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestHashIndex_Delete(t *testing.T) {
	index := NewStringHashIndex(`foo9`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := index.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	if err := index.Delete(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if len(data) > 0 {
		t.Fail()
	}
}

func TestHashIndex_Name(t *testing.T) {
	tests := []struct {
		name string
		idx  Index
		want string
	}{
		{
			name: `name`,
			idx:  NewStringHashIndex(`foo`, nil),
			want: `foo`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.idx.String(); got != tt.want {
				t.Errorf("Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHashIndex_Read(t *testing.T) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := index.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{`100`}) {
		t.Errorf("expect []interface{}{`100`} have %#v", data)
	}
}

func TestHashIndex_Write(t *testing.T) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := index.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{`100`}) {
		t.Errorf("expect []interface{}{`100`} have %#v", data)
	}
}

func TestHashIndex_WriteUuidKey(t *testing.T) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	uid := uuid.New()

	if err := index.Write(uid, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{uid}) {
		t.Errorf("expect []interface{}{`100`} have %#v", data)
	}
}
