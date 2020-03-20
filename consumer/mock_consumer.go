package consumer

import (
	"context"
	"github.com/google/uuid"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/offsets"
	"log"
	"sync"
	"time"
)

type MockConsumerBuilder struct {
	Builder
	topics *admin.Topics
}

func NewMockConsumerBuilder(topics *admin.Topics) Builder {
	return &MockConsumerBuilder{
		Builder: NewBuilder(),
		topics:  topics,
	}
}

func (mb *MockConsumerBuilder) Build(options ...BuilderOption) (Consumer, error) {
	return NewMockConsumer(mb.topics), nil
}

type MockPartitionConsumerBuilder struct {
	PartitionConsumerBuilder
	offsets offsets.Manager
	topics  *admin.Topics
}

func NewMockPartitionConsumerBuilder(topics *admin.Topics, offsets offsets.Manager) PartitionConsumerBuilder {
	return &MockPartitionConsumerBuilder{
		PartitionConsumerBuilder: NewPartitionConsumerBuilder(),
		topics:                   topics,
		offsets:                  offsets,
	}
}

func (mb *MockPartitionConsumerBuilder) Build(options ...BuilderOption) (PartitionConsumer, error) {
	return NewMockPartitionConsumer(mb.topics, mb.offsets), nil
}

type mockConsumer struct {
	topics         *admin.Topics
	wg             *sync.WaitGroup
	fetchInterval  time.Duration
	fetchBatchSize int
	partitions     chan Partition
	closing        chan bool
	InitialOffset  Offset
}

func NewMockConsumer(topics *admin.Topics) *mockConsumer {
	return &mockConsumer{
		topics:         topics,
		fetchInterval:  100 * time.Microsecond,
		fetchBatchSize: 50,
		wg:             new(sync.WaitGroup),
		InitialOffset:  Earliest,
		closing:        make(chan bool, 1),
	}
}

func (m *mockConsumer) Consume(topics []string, handler ReBalanceHandler) (chan Partition, error) {
	tps := make(map[string]*mockConsumerPartition)
	var assigned []TopicPartition

	for _, topic := range topics {
		tp, err := m.topics.Topic(topic)
		if err != nil {
			return nil, err
		}
		for p := range tp.Partitions() {
			tp := TopicPartition{
				Topic:     topic,
				Partition: int32(p),
			}
			assigned = append(assigned, tp)
		}
	}
	if err := handler.OnPartitionAssigned(context.Background(), assigned); err != nil {
		return nil, err
	}
	m.partitions = make(chan Partition, len(assigned))
	for _, tp := range assigned {
		consumerPartition := &mockConsumerPartition{
			tp:      tp,
			records: make(chan *data.Record, 10000),
		}
		tps[tp.String()] = consumerPartition
		m.partitions <- consumerPartition
		m.wg.Add(1)
		go m.consume(consumerPartition)
	}

	return m.partitions, nil
}

func (m *mockConsumer) Errors() <-chan *Error {
	return make(<-chan *Error, 1)
}

func (m *mockConsumer) Close() error {
	m.closing <- true
	m.wg.Wait()
	close(m.partitions)
	return nil
}

func (m *mockConsumer) consume(partition *mockConsumerPartition) {
	pt := m.topics.Topics()[partition.tp.Topic].Partitions()[partition.tp.Partition]

	offset := int64(m.InitialOffset)
LOOP:
	for {
		select {
		case <-m.closing:
			break LOOP
		default:
		}

		time.Sleep(m.fetchInterval)

		records, err := pt.Fetch(offset, m.fetchBatchSize)
		if err != nil {
			log.Fatal(err)
		}

		if len(records) < 1 {
			continue
		}

		for _, msg := range records {
			partition.records <- &data.Record{
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				UUID:      uuid.New(),
				Headers:   msg.Headers,
			}
		}

		offset = records[len(records)-1].Offset + 1

	}
	close(partition.records)
	m.wg.Done()
}
