package store

import (
	"fmt"
	"github.com/google/uuid"
	"reflect"
	"strings"
	"testing"
)

func TestNewUUIDHashIndexIndex(t *testing.T) {
	var mapper func(key, val interface{}) (idx uuid.UUID)
	index := NewUuidHashIndex(`foo`, mapper)
	type args struct {
		name   string
		mapper func(key, val interface{}) (idx uuid.UUID)
	}
	tests := []struct {
		name string
		args args
		want Index
	}{
		{name: `new`, args: struct {
			name   string
			mapper func(key, val interface{}) (idx uuid.UUID)
		}{name: `foo`, mapper: mapper}, want: index},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewUuidHashIndex(tt.args.name, tt.args.mapper); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStringHashIndex() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestUUIDHashIndex_Delete(t *testing.T) {
	uid1 := uuid.New()
	uid2 := uuid.New()
	index := NewUuidHashIndex(`foo`, func(key, val interface{}) (idx uuid.UUID) {
		uid, _ := uuid.Parse(strings.Split(val.(string), `,`)[0])
		return uid
	})

	if err := index.Write(uid1, fmt.Sprintf(`%s,%s`, uid1.String(), uid2.String())); err != nil {
		t.Error(err)
	}

	if err := index.Delete(uid1, fmt.Sprintf(`%s,%s`, uid1.String(), uid2.String())); err != nil {
		t.Error(err)
	}

	data, err := index.Read(uid1)
	if err != nil {
		t.Error(err)
	}

	if len(data) > 0 {
		t.Fail()
	}
}

func TestUUIDHashIndex_Name(t *testing.T) {
	tests := []struct {
		name string
		idx  Index
		want string
	}{
		{
			name: `name`,
			idx:  NewUuidHashIndex(`foo`, nil),
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

func TestUUIDHashIndex_Read(t *testing.T) {
	uid1 := uuid.New()
	uid2 := uuid.New()
	index := NewUuidHashIndex(`foo`, func(key, val interface{}) (idx uuid.UUID) {
		uid, _ := uuid.Parse(strings.Split(val.(string), `,`)[0])
		return uid
	})

	if err := index.Write(uid1, fmt.Sprintf(`%s,%s`, uid1.String(), uid2.String())); err != nil {
		t.Error(err)
	}

	data, err := index.Read(uid1)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{uid1}) {
		t.Errorf("expect []interface{}{`100`} have %#v", data)
	}
}

func TestUUIDHashIndex_Write(t *testing.T) {
	uid1 := uuid.New()
	uid2 := uuid.New()
	index := NewUuidHashIndex(`foo`, func(key, val interface{}) (idx uuid.UUID) {
		uid, _ := uuid.Parse(strings.Split(val.(string), `,`)[0])
		return uid
	})

	if err := index.Write(uid1, fmt.Sprintf(`%s,%s`, uid1.String(), uid2.String())); err != nil {
		t.Error(err)
	}

	data, err := index.Read(uid1)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{uid1}) {
		t.Errorf("expect []interface{}{`100`} have %#v", data)
	}
}
