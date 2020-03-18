package dlq

import (
	"context"
	"errors"
	"fmt"
	"github.com/tryfix/kstream/data"
	kContext "github.com/tryfix/kstream/k-stream/context"
	"github.com/tryfix/kstream/producer"
)

type DqlType int

const DqlGlobal DqlType = 1
const DqlPerTopic DqlType = 2

type DLQ interface {
	Publish(ctx context.Context, record *data.Record) error
}

type Builder func() (DLQ, error)

type dlq struct {
	producer producer.Producer
	options  *Options
}

type Options struct {
	BootstrapServers []string
	Topic            string
	TopicFormat      string
	Type             DqlType
	Producer         producer.Producer
}

func NewDLQ(options *Options) (DLQ, error) {
	/*p, err := producer.DefaultBuilder(&producer.Options{
		BootstrapServers: options.BootstrapServers,
		Partitioner:      producer.Random,
	})
	if err != nil {
		return nil, err
	}*/

	return &dlq{
		options: options,
		//producer: p,
	}, nil
}

func (dq *dlq) Publish(ctx context.Context, record *data.Record) error {
	if _, _, err := dq.producer.Produce(ctx, record); err != nil {
		return err
	}

	return nil
}

func (dq *dlq) prepareMessage(ctx context.Context, key []byte, value []byte) (*data.Record, error) {
	kCtx, ok := ctx.(*kContext.Context)
	if !ok {
		return nil, errors.New(`k-stream.DLQ.Publish: published message context should be the type of kstream.Context`)
	}

	return &data.Record{
		Key:       key,
		Value:     value,
		Partition: kContext.Meta(kCtx).Partition,
		Topic:     dq.topic(kContext.Meta(kCtx).Topic),
	}, nil
}

func (dq *dlq) topic(topic string) string {
	if dq.options.Type == DqlPerTopic {
		return fmt.Sprintf(dq.options.TopicFormat, topic)
	}

	return dq.options.Topic
}
