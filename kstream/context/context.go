package context

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/traceable-context"
	"time"
)

var recordMeta = `rc_meta`

type RecordMeta struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   data.RecordHeaders
}

type Context struct {
	context.Context
}

func FromRecord(parent context.Context, record *data.Record) context.Context {
	return traceable_context.WithValue(parent, &recordMeta, &RecordMeta{
		Topic:     record.Topic,
		Offset:    record.Offset,
		Partition: record.Partition,
		Timestamp: record.Timestamp,
		Headers:   record.Headers,
	})
}

func RecordFromContext(ctx context.Context, key []byte, val []byte) (*data.Record, error) {
	if c, ok := ctx.(*Context); ok {

		meta := Meta(c)

		return &data.Record{
			Topic:     meta.Topic,
			Partition: meta.Partition,
			Offset:    meta.Offset,
			Timestamp: meta.Timestamp,
			Key:       key,
			Value:     val,
			Headers:   meta.Headers,
		}, nil
	}

	return nil, errors.New(`invalid context expected [k-stream.context]`)
}

func Meta(ctx context.Context) *RecordMeta {
	if meta, ok := ctx.Value(&recordMeta).(*RecordMeta); ok {
		return meta
	}
	panic(`k-stream.context meta not available`)
}
