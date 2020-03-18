package consumer

import (
	"fmt"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/k-stream/offsets"
	"testing"
)

func TestMockPartitionConsumer_Consume(t *testing.T) {
	mocksTopics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: mocksTopics,
	}
	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`tp1`: {
			Name:              "tp1",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}); err != nil {
		t.Error(err)
	}
	tp, _ := mocksTopics.Topic(`tp1`)
	pt, _ := tp.Partition(0)

	t.Run(`ZeroMessage`, func(t *testing.T) {
		con := NewMockPartitionConsumer(mocksTopics, &offsets.MockManager{Topics: mocksTopics})
		ch, err := con.Consume(`tp1`, 0, Earliest)
		if err != nil {
			t.Error(err)
		}

		var count int

	L:
		for msg := range ch {
			if _, ok := msg.(*PartitionEnd); ok {
				break L
			}
			count++
		}

		if count != 0 {
			t.Error(`expected 0 have `, count)
			t.Fail()
		}
	})

	for i := 1; i <= 3333; i++ {
		err := pt.Append(&data.Record{
			Key:   []byte(fmt.Sprint(i)),
			Value: []byte(`v`),
			Topic: "tp1",
		})
		if err != nil {
			t.Error(err)
		}
	}

	t.Run(`Earliest`, func(t *testing.T) {
		con := NewMockPartitionConsumer(mocksTopics, &offsets.MockManager{Topics: mocksTopics})
		ch, err := con.Consume(`tp1`, 0, Earliest)
		if err != nil {
			t.Error(err)
		}

		var count int

	L:
		for msg := range ch {
			if _, ok := msg.(*PartitionEnd); ok {
				break L
			}
			count++
		}

		if count != 3333 {
			t.Error(`expected 3333 have `, count)
			t.Fail()
		}
	})

	t.Run(`Latest`, func(t *testing.T) {
		con := NewMockPartitionConsumer(mocksTopics, &offsets.MockManager{Topics: mocksTopics})
		ch, err := con.Consume(`tp1`, 0, Latest)
		if err != nil {
			t.Error(err)
		}

		var count int

	L:
		for msg := range ch {
			if _, ok := msg.(*PartitionEnd); ok {
				break L
			}
			count++
		}

		if count != 0 {
			t.Error(`expected 0 have `, count)
			t.Fail()
		}
	})

}
