package encoding

import (
	"reflect"
	"testing"
)

func TestIntEncoder_Decode(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{name: `should_decode`, args: args{data: []byte(`1`)}, want: 1, wantErr: false},
		{name: `should_return_error`, args: args{data: []byte(`ss`)}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := IntEncoder{}
			got, err := in.Decode(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntEncoder_Encode(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{name: `should_decode`, args: args{100}, want: []byte(`100`), wantErr: false},
		{name: `should_return_error`, args: args{nil}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := IntEncoder{}
			got, err := in.Encode(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Encode() got = %v, want %v", got, tt.want)
			}
		})
	}
}
