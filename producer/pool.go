package producer

import (
	"context"
	"github.com/tryfix/kstream/data"
	"hash"
	"hash/fnv"
)

type Pool struct {
	NumOfWorkers int64
	producers    map[int64]Producer
	hasher       hash.Hash32
}

func NewPool(NumOfWorkers int, builder Builder) (*Pool, error) {

	producers := make(map[int64]Producer)

	pool := &Pool{
		NumOfWorkers: int64(NumOfWorkers),
		producers:    producers,
		hasher:       fnv.New32a(),
	}

	for i := 0; i < NumOfWorkers; i++ {
		p, err := builder(new(Config))
		if err != nil {
			return nil, err
		}
		pool.producers[int64(i)] = p
	}

	return pool, nil
}

func (p *Pool) Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error) {
	producer, err := p.producer(message.Key)
	if err != nil {
		return 0, 0, err
	}

	return producer.Produce(ctx, message)
}

func (p *Pool) ProduceBatch(ctx context.Context, messages []*data.Record) error {
	producer, err := p.producer(messages[0].Key)
	if err != nil {
		return err
	}

	return producer.ProduceBatch(ctx, messages)
}

func (p *Pool) Close() error {
	for _, producer := range p.producers {
		if err := producer.Close(); err != nil {
			println(err)
		}
	}

	return nil
}

func (p *Pool) producer(key []byte) (Producer, error) {
	p.hasher.Reset()
	_, err := p.hasher.Write(key)
	if err != nil {
		return nil, err
	}

	w := int64(p.hasher.Sum32()) % p.NumOfWorkers

	return p.producers[w], nil
}
