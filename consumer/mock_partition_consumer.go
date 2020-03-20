package consumer

import (
	"github.com/google/uuid"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/offsets"
	"log"
	"time"
)

type mockPartitionConsumer struct {
	topics         *admin.Topics
	offsets        offsets.Manager
	fetchInterval  time.Duration
	closing        chan bool
	closed         chan bool
	fetchBatchSize int
	events         chan Event
}

func NewMockPartitionConsumer(topics *admin.Topics, offsets offsets.Manager) *mockPartitionConsumer {
	return &mockPartitionConsumer{
		topics:        topics,
		fetchInterval: 100 * time.Microsecond,
		//fetchInterval:  1 * time.Second,
		fetchBatchSize: 1000,
		closed:         make(chan bool, 1),
		closing:        make(chan bool, 1),
		offsets:        offsets,
		events:         make(chan Event, 100),
	}
}

func (m *mockPartitionConsumer) Consume(topic string, partition int32, offset Offset) (<-chan Event, error) {
	go m.consume(topic, partition, offset)
	return m.events, nil
}

func (m *mockPartitionConsumer) consume(topic string, partition int32, offset Offset) {
	pt := m.topics.Topics()[topic].Partitions()[int(partition)]

	var currentOffset = int64(offset)

	if offset == -1 {
		currentOffset = pt.Latest() + 1
	}

LOOP:
	for {
		select {
		case <-m.closing:
			break LOOP
		default:

		}

		time.Sleep(m.fetchInterval)

		records, err := pt.Fetch(currentOffset, m.fetchBatchSize)
		if err != nil {
			log.Fatal(err)
		}

		if len(records) < 1 {
			m.events <- &PartitionEnd{}
			continue
		}

		partitionEnd, err := m.offsets.GetOffsetLatest(topic, partition)
		if err != nil {
			log.Fatal(err)
		}

		for _, msg := range records {
			m.events <- &data.Record{
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				UUID:      uuid.New(),
				Headers:   msg.Headers,
			}

			//if highWatermark == 0 || highWatermark-1 == msg.Offset {
			if msg.Offset == partitionEnd {
				m.events <- &PartitionEnd{}
			}
		}

		currentOffset = records[len(records)-1].Offset + 1
	}

	m.closed <- true
}

func (m *mockPartitionConsumer) Errors() <-chan *Error {
	return make(chan *Error)
}

func (m *mockPartitionConsumer) Close() error {
	m.closing <- true
	<-m.closed
	close(m.events)
	return nil
}

func (m *mockPartitionConsumer) Id() string {
	panic("implement me")
}
