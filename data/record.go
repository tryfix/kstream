package data

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"time"
)

type RecordHeader struct {
	Key   []byte
	Value []byte
}

type Record struct {
	Key, Value     []byte
	Topic          string
	Partition      int32
	Offset         int64
	Timestamp      time.Time              // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time              // only set if kafka is version 0.10+, outer (compressed) block timestamp
	Headers        []*sarama.RecordHeader // only set if kafka is version 0.11+
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
