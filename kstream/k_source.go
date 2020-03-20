package kstream

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/kstream/kstream/topology"
)

type kSource struct {
	Id         int32
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
	name       string
	topic      string
}

type kSourceBuilder struct {
	keyEncoderBuilder encoding.Builder
	valEncoderBuilder encoding.Builder
	name              string
	topic             string
	info              map[string]string
}

func (b *kSourceBuilder) Build() (topology.Source, error) {
	return &kSource{
		name:       b.name,
		topic:      b.topic,
		keyEncoder: b.keyEncoderBuilder(),
		valEncoder: b.valEncoderBuilder(),
	}, nil
}

func (b *kSourceBuilder) Name() string {
	return b.name
}

func (b *kSourceBuilder) SourceType() string {
	return `kafka`
}

func (b *kSourceBuilder) Info() map[string]string {
	return b.info
}

func (s *kSource) Name() string {
	return s.name
}

func (s *kSource) Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
	return s.decodeRecord(kIn, vIn)
}

func (s *kSource) decodeRecord(key []byte, val []byte) (interface{}, interface{}, error) {
	k, err := s.keyEncoder.Decode(key)
	if err != nil {
		return nil, nil, errors.WithPrevious(err, `key decode error`)
	}

	v, err := s.valEncoder.Decode(val)
	if err != nil {
		return nil, nil, errors.WithPrevious(err, `value decode error`)
	}

	return k, v, nil
}

func (s *kSource) Close() {}

func (s *kSource) Next() bool {
	return true
}

func (s *kSource) Type() topology.Type {
	return topology.TypeSource
}
