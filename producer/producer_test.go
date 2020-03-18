package producer

import (
	"context"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/data"
	"testing"
)

func setupMockTopics(t *testing.T, topics *admin.Topics) {
	if err := topics.AddTopic(&admin.MockTopic{
		Name: "",
		Meta: &admin.Topic{
			Name:          "testing",
			NumPartitions: 2,
		},
	}); err != nil {
		t.Error(err)
	}

}

func TestMockProducer_Produce(t *testing.T) {
	topics := admin.NewMockTopics()
	setupMockTopics(t, topics)
	producer := NewMockProducer(topics)
	msg := &data.Record{
		Key:       []byte(`100`),
		Value:     []byte(`100`),
		Partition: 1,
	}

	p, o, err := producer.Produce(context.Background(), msg)
	if err != nil {
		t.Error(err)
	}

	if p != 0 || o != 0 {
		t.Fail()
	}
}

func TestMockProducer_ProduceBatch(t *testing.T) {
	topics := admin.NewMockTopics()
	setupMockTopics(t, topics)
	producer := NewMockProducer(topics)

	msg1 := &data.Record{
		Key:       []byte(string(`100`)),
		Value:     []byte(string(`100`)),
		Partition: 1,
	}

	msg2 := *msg1
	msg2.Key = []byte(string(`100`))

	err := producer.ProduceBatch(context.Background(), []*data.Record{msg1, &msg2})
	if err != nil {
		t.Error(err)
	}
}
