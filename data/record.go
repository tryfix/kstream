package data

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"time"
)

//type RecordHeaders interface {
//	Read(name []byte) []byte
//	All() []*sarama.RecordHeader
//}

type RecordHeaders []*sarama.RecordHeader

func (h RecordHeaders) Read(name []byte) []byte {
	for _, header := range h {
		if bytes.Equal(header.Key, name) {
			return header.Value
		}
	}

	return nil
}

func (h RecordHeaders) All() []*sarama.RecordHeader {
	return h
}

type Record struct {
	Key, Value     []byte
	Topic          string
	Partition      int32
	Offset         int64
	Timestamp      time.Time     // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time     // only set if kafka is version 0.10+, outer (compressed) block timestamp
	Headers        RecordHeaders // only set if kafka is version 0.11+
	UUID           uuid.UUID
}

func (r *Record) String() string {
	return fmt.Sprintf(`%s_%d_%d`, r.Topic, r.Partition, r.Offset)
}

func (r *Record) RecordKey() interface{} {
	return r.Key
}

func (r *Record) RecordValue() interface{} {
	return r.Value
}
