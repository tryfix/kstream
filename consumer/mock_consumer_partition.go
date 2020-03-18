package consumer

import (
	"github.com/tryfix/kstream/data"
)

type mockConsumerPartition struct {
	tp      TopicPartition
	records chan *data.Record
}

func (m *mockConsumerPartition) Wait() chan<- bool {
	return nil
}

func (m *mockConsumerPartition) Records() <-chan *data.Record {
	return m.records
}

func (m *mockConsumerPartition) Partition() TopicPartition {
	return m.tp
}

func (m *mockConsumerPartition) MarkOffset(offset int64) {}

func (m *mockConsumerPartition) CommitOffset(*data.Record) error {
	return nil
}
