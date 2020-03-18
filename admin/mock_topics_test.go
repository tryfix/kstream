package admin

import (
	"fmt"
	"github.com/tryfix/kstream/data"
	"testing"
)

func TestMockPartition_Latest(t *testing.T) {
	mocksTopics := NewMockTopics()
	kafkaAdmin := &MockKafkaAdmin{
		Topics: mocksTopics,
	}
	if err := kafkaAdmin.CreateTopics(map[string]*Topic{
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

	if pt.Latest() != 3332 {
		t.Fail()
	}
}
