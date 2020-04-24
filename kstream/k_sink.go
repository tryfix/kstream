package kstream

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	kContext "github.com/tryfix/kstream/kstream/context"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/topology"
	"github.com/tryfix/kstream/producer"
	"time"
)

type SinkRecord struct {
	Key, Value interface{}
	Timestamp  time.Time          // only set if kafka is version 0.10+, inner message timestamp
	Headers    data.RecordHeaders // only set if kafka is version 0.11+
}

type KSink struct {
	Id                    int32
	KeyEncoder            encoding.Encoder
	ValEncoder            encoding.Encoder
	Producer              producer.Producer
	ProducerBuilder       producer.Builder
	name                  string
	TopicPrefix           string
	topic                 topic
	Repartitioned         bool
	info                  map[string]string
	KeyEncoderBuilder     encoding.Builder
	ValEncoderBuilder     encoding.Builder
	recordTransformer     func(ctx context.Context, in SinkRecord) (out SinkRecord, err error)
	recordHeaderExtractor func(ctx context.Context, in SinkRecord) (data.RecordHeaders, error)
	tombstoneFiler        func(ctx context.Context, in SinkRecord) (tombstone bool)
}

func (s *KSink) Childs() []topology.Node {
	return []topology.Node{}
}

func (s *KSink) ChildBuilders() []topology.NodeBuilder {
	return []topology.NodeBuilder{}
}

func (s *KSink) Build() (topology.Node, error) {
	p, err := s.ProducerBuilder(&producer.Config{
		//id: producer.NewProducerId(s.topic(s.topic(s.TopicPrefix))),
	})
	if err != nil {
		return nil, errors.WithPrevious(err, `producer build failed`)
	}

	s.Producer = p
	s.KeyEncoder = s.KeyEncoderBuilder()
	s.ValEncoder = s.ValEncoderBuilder()

	return s, nil
}

func (s *KSink) AddChildBuilder(builder topology.NodeBuilder) {
	panic("implement me")
}

func (s *KSink) AddChild(node topology.Node) {
	panic("implement me")
}

//type kSinkBuilder struct {
//	keyEncoderBuilder encoding.Builder
//	valEncoderBuilder encoding.Builder
//	producerBuilder   producer.Builder
//	name              string
//	info              map[string]string
//	topic             string
//}

//func (b *kSinkBuilder) AddChildBuilder(builder node.NodeBuilder) {
//	panic("implement me")
//}
//
//func (b *kSinkBuilder) Build() (node.Node, error) {
//
//	p, err := b.producerBuilder(&producer.Options{
//		id: producer.NewProducerId(b.topic),
//	})
//	if err != nil {
//		return nil, errors.WithPrevious(err,  `cannot Build producer`)
//	}
//
//	return &kSink{
//		keyEncoder: b.keyEncoderBuilder(),
//		valEncoder: b.valEncoderBuilder(),
//		producer:   p,
//		name:       b.name,
//		topic:      b.topic,
//	}, nil
//}

type SinkOption func(sink *KSink)

func (s *KSink) applyOptions(options ...SinkOption) {
	for _, option := range options {
		option(s)
	}
}

func (s *KSink) Name() string {
	return `sink_` + s.topic(s.TopicPrefix)
}

func (*KSink) Next() bool {
	return false
}

func (s *KSink) SinkType() string {
	return `kafka`
}

func (*KSink) Type() topology.Type {
	return topology.TypeSink
}

func (s *KSink) Info() map[string]string {
	return map[string]string{
		`topic`: s.topic(s.TopicPrefix),
	}
}

// Deprecated: Please use SinkWithProducer instead
func WithProducer(p producer.Builder) SinkOption {
	return func(sink *KSink) {
		sink.ProducerBuilder = p
	}
}

func SinkWithProducer(p producer.Builder) SinkOption {
	return func(sink *KSink) {
		sink.ProducerBuilder = p
	}
}

// Deprecated: Please use SinkWithRecordHeaderExtractor instead
func WithCustomRecord(f func(ctx context.Context, in SinkRecord) (out SinkRecord, err error)) SinkOption {
	return func(sink *KSink) {
		sink.recordTransformer = f
	}
}

func SinkWithRecordHeaderExtractor(f func(ctx context.Context, in SinkRecord) (headers data.RecordHeaders, err error)) SinkOption {
	return func(sink *KSink) {
		sink.recordHeaderExtractor = f
	}
}

func SinkWithTombstoneFilter(f func(ctx context.Context, in SinkRecord) (tombstone bool)) SinkOption {
	return func(sink *KSink) {
		sink.tombstoneFiler = f
	}
}

func withPrefixTopic(topic topic) SinkOption {
	return func(sink *KSink) {
		sink.topic = topic
	}
}

func NewKSinkBuilder(name string, id int32, topic topic, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) *KSink {

	builder := &KSink{
		ValEncoderBuilder: valEncoder,
		KeyEncoderBuilder: keyEncoder,
		topic:             topic,
		name:              name,
		Id:                id,
		tombstoneFiler: func(ctx context.Context, in SinkRecord) (tombstone bool) {
			return false
		},
		recordHeaderExtractor: func(ctx context.Context, in SinkRecord) (out data.RecordHeaders, err error) {
			return in.Headers, nil
		},
		recordTransformer: func(ctx context.Context, in SinkRecord) (out SinkRecord, err error) {
			return in, nil
		},
	}

	builder.applyOptions(options...)
	return builder
}

func (s *KSink) Close() error {
	return nil
}

func (s *KSink) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {

	record := new(data.Record)
	record.Timestamp = time.Now()
	record.Topic = s.topic(s.TopicPrefix)

	skinRecord := SinkRecord{
		Key:       kIn,
		Value:     vIn,
		Timestamp: record.Timestamp,
		Headers:   kContext.Meta(ctx).Headers.All(),
	}

	// Deprecated: apply custom record transformations
	customRecord, err := s.recordTransformer(ctx, skinRecord)
	if err != nil {
		return nil, nil, false, err
	}
	skinRecord.Key = customRecord.Key
	skinRecord.Value = customRecord.Value
	skinRecord.Headers = customRecord.Headers
	skinRecord.Timestamp = customRecord.Timestamp

	// apply data record headers
	headers, err := s.recordHeaderExtractor(ctx, skinRecord)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `record extract failed`)
	}
	skinRecord.Headers = headers

	keyByt, err := s.KeyEncoder.Encode(skinRecord.Key)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `sink key encode error`)
	}
	record.Key = keyByt
	// if the record value is null or marked as null with a tombstoneFiler set the record value as null
	tombstoned := s.tombstoneFiler(ctx, skinRecord)
	if skinRecord.Key == nil || tombstoned {
		record.Value = nil
	} else {
		valByt, err := s.ValEncoder.Encode(skinRecord.Value)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `sink value encode error`)
		}
		record.Value = valByt
	}

	record.Headers = skinRecord.Headers

	if _, _, err := s.Producer.Produce(ctx, record); err != nil {
		return nil, nil, false, err
	}

	return nil, nil, true, nil
}

func (s *KSink) ID() int32 {
	return s.Id
}
