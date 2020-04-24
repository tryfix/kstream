package context

import (
	"context"
	"github.com/tryfix/kstream/data"
	"reflect"
	"testing"
)

func TestFromRecord(t *testing.T) {
	//type args struct {
	//	parent context.Context
	//	record *data.Record
	//}
	//
	//ctx := context.WithValue(context.Background(), `foo`, `bar`)
	//kafkaRec := &data.Record{
	//	Key:            nil,
	//	Value:          nil,
	//	Topic:          "test",
	//	Partition:      0,
	//	Offset:         0,
	//	Timestamp:      time.Time{},
	//	BlockTimestamp: time.Time{},
	//	RecordHeaders:        nil,
	//	UUID:           uuid.UUID{},
	//}
	//
	//tests := []struct {
	//	name string
	//	args args
	//	want context.Context
	//}{
	//	{name: `default`, args: args{
	//		parent: ctx,
	//		record: nil,
	//	}, want: Context{}},
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		if got := FromRecord(tt.args.parent, tt.args.record); !reflect.DeepEqual(got, tt.want) {
	//			t.Errorf("FromRecord() = %v, want %v", got, tt.want)
	//		}
	//	})
	//}
}

func TestMeta(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
		want *RecordMeta
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Meta(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Meta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordFromContext(t *testing.T) {
	type args struct {
		ctx context.Context
		key []byte
		val []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *data.Record
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RecordFromContext(tt.args.ctx, tt.args.key, tt.args.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("RecordFromContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RecordFromContext() got = %v, want %v", got, tt.want)
			}
		})
	}
}
