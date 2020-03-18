package changelog

import (
	"context"
	"crypto/sha1"
	"github.com/tryfix/kstream/data"
	"sync"
)

type mockChangelog struct {
	data       map[string]*data.Record
	mu         *sync.Mutex
	buffer     *Buffer
	bufferSize int
}

func NewMockChangelog(bufferSize int) Changelog {
	return &mockChangelog{
		//buffer:     NewBuffer(),
		bufferSize: bufferSize,
		mu:         new(sync.Mutex),
		data:       make(map[string]*data.Record),
	}
}

func (c *mockChangelog) ReadAll(ctx context.Context) ([]*data.Record, error) {
	var data []*data.Record
	for _, rec := range c.data {
		data = append(data, rec)
	}
	return data, nil
}

func (c *mockChangelog) Put(ctx context.Context, record *data.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.buffer.records) >= c.bufferSize {
		c.PutAll(ctx, c.buffer.records)
		return nil
	}

	c.buffer.Store(record)
	return nil
}

func (c *mockChangelog) PutAll(ctx context.Context, records []*data.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rec := range records {
		c.data[c.hash(rec.Key)] = rec
	}
	return nil
}

func (c *mockChangelog) Delete(ctx context.Context, record *data.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, c.hash(record.Key))
	return nil
}

func (c *mockChangelog) DeleteAll(ctx context.Context, records []*data.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rec := range records {
		delete(c.data, c.hash(rec.Key))
	}

	return nil
}

func (c *mockChangelog) Info() map[string]interface{} {
	panic("implement me")
}

func (c *mockChangelog) Close() {
	c.buffer = nil
	c.mu = nil
	c.data = nil
}

func (c *mockChangelog) hash(k []byte) string {
	ha := sha1.New()
	ha.Write(k)
	return string(ha.Sum(nil))
}
