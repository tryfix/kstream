package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/data"
)

type Partition interface {
	Records() <-chan *data.Record
	Partition() TopicPartition
	MarkOffset(offset int64)
	CommitOffset(*data.Record) error
}

type partition struct {
	records      chan *data.Record
	groupSession sarama.ConsumerGroupSession
	partition    TopicPartition
}

func newPartition(tp TopicPartition) *partition {
	return &partition{
		records:   make(chan *data.Record, 1),
		partition: tp,
	}
}

func (p *partition) Records() <-chan *data.Record {
	return p.records
}

func (p *partition) Partition() TopicPartition {
	return p.partition
}

func (p *partition) MarkOffset(offset int64) {
	p.groupSession.MarkOffset(p.partition.Topic, p.partition.Partition, offset+1, ``)
}

func (p *partition) CommitOffset(r *data.Record) error {
	p.groupSession.MarkOffset(r.Topic, r.Partition, r.Offset+1, ``)
	return nil
}

func (p *partition) close() {
	close(p.records)
}
