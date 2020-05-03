package store

import (
	"reflect"
	"strings"
	"testing"
)

func TestNewAnyHashIndex(t *testing.T) {
	var mapper func(key, val interface{}) (idx string)
	index := NewAnyHashIndex(`foo`, mapper)
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
			if got := NewAnyHashIndex(tt.args.name, tt.args.mapper); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStringHashIndex() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAnyHashIndex_Delete(t *testing.T) {
	index := NewAnyHashIndex(`foo9`, func(key, val interface{}) (idx string) {
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

func TestAnyHashIndex_Name(t *testing.T) {
	tests := []struct {
		name string
		idx  Index
		want string
	}{
		{
			name: `name`,
			idx:  NewAnyHashIndex(`foo`, nil),
			want: `foo`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.idx.Name(); got != tt.want {
				t.Errorf("Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnyHashIndex_Read(t *testing.T) {
	index := NewAnyHashIndex(`foo`, func(key, val interface{}) (idx string) {
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

func TestAnyHashIndex_Write(t *testing.T) {
	index := NewAnyHashIndex(`foo`, func(key, val interface{}) (idx string) {
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
