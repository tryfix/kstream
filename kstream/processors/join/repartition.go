package join

import (
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/encoding"
)

type Side int

const (
	LeftSide Side = iota + 1
	RightSide
)

type RepartitionTopic struct {
	Name              string
	Suffix            string
	ReplicationFactor int
	NumOfPartitions   int
	MinInSycReplicas  int
}

type Repartition struct {
	Enable       bool
	StreamSide   Side
	KeyEncoder   encoding.Builder
	ValueEncoder encoding.Builder
	Topic        RepartitionTopic
}

type RepartitionOptions struct {
	LeftTopic        func(string) string
	RightTopic       func(string) string
	LeftRepartition  Repartition
	RightRepartition Repartition
}

type RepartitionOption func(sink *RepartitionOptions)

func RepartitionLeftStream(keyEncodingBuilder, valueEncodingBuilder encoding.Builder) RepartitionOption {
	return func(opts *RepartitionOptions) {
		opts.LeftRepartition = Repartition{
			Enable:       true,
			StreamSide:   LeftSide,
			KeyEncoder:   keyEncodingBuilder,
			ValueEncoder: valueEncodingBuilder,
		}
	}
}

func RepartitionRightStream(keyEncodingBuilder, valueEncodingBuilder encoding.Builder) RepartitionOption {
	return func(opts *RepartitionOptions) {
		opts.RightRepartition = Repartition{
			Enable:       true,
			StreamSide:   RightSide,
			KeyEncoder:   keyEncodingBuilder,
			ValueEncoder: valueEncodingBuilder,
		}
	}
}

func (iOpts *RepartitionOptions) Apply(options ...RepartitionOption) {
	for _, o := range options {
		o(iOpts)
	}
}

func (r Repartition) Validate(s Side) error {
	fmt.Println(r)
	if r.StreamSide != s {
		return errors.New(`stream side is not compatible`)
	}
	if r.KeyEncoder == nil {
		return errors.New(`repartition key encoder can not be nil`)
	}
	if r.ValueEncoder == nil {
		return errors.New(`repartition value encoder can not be nil`)
	}
	//if r.Topic.Name == `` {
	//	return errors.New( `repartition topic can not be empty`)
	//}

	return nil
}
