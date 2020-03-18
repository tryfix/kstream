package changelog

import (
	"encoding/binary"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"sync"
)

type cacheManager struct {
	caches             map[string]*cache
	mu                 *sync.Mutex
	backendBuilder     backend.Builder
	cacheOffsetStorage backend.Backend
}

func newCacheManager(backendBuilder backend.Builder) (*cacheManager, error) {
	m := &cacheManager{
		caches:         make(map[string]*cache),
		mu:             new(sync.Mutex),
		backendBuilder: backendBuilder,
	}

	offsetBackend, err := backendBuilder(`__changelog_cache_offsets`)
	if err != nil {
		return nil, err
	}

	//if !offsetBackend.Persistent() {
	//	return nil, errors.New( `only persistent backend are supported`)
	//}

	m.cacheOffsetStorage = offsetBackend
	return m, nil
}

func (m *cacheManager) getCache(tp consumer.TopicPartition) (*cache, error) {
	if c, ok := m.caches[tp.String()]; ok {
		return c, nil
	}

	b, err := m.backendBuilder(`__changelog_cache_` + tp.String())
	if err != nil {
		return nil, err
	}

	cache := new(cache)
	cache.tp = tp
	cache.backend = b
	cache.offsetBackend = m.cacheOffsetStorage

	m.mu.Lock()
	m.caches[tp.String()] = cache
	m.mu.Unlock()

	return cache, nil
}

type cache struct {
	backend       backend.Backend
	offsetBackend backend.Backend
	tp            consumer.TopicPartition
}

func (c *cache) Flush() error {
	itr := c.backend.Iterator()
	for itr.Valid() {
		if err := c.backend.Delete(itr.Key()); err != nil {
			return errors.WithPrevious(err, `cache flush failed`)
		}
		itr.Next()
	}
	return nil
}

func (c *cache) Put(record *data.Record) error {

	if len(record.Value) < 1 {
		if err := c.backend.Delete(record.Key); err != nil {
			return err
		}
	} else {
		if err := c.backend.Set(record.Key, record.Value, 0); err != nil {
			return err
		}
	}
	// update current offset on backend
	return c.offsetBackend.Set([]byte(c.offsetKeyPrefix()), c.encodeOffset(record.Offset), 0)

}

func (c *cache) offsetKeyPrefix() string {
	return `__changelog_offset_cache_last_synced_` + c.tp.String()
}

func (c *cache) ReadAll() []*data.Record {
	var records []*data.Record

	i := c.backend.Iterator()
	i.SeekToFirst()
	for i.Valid() {
		record := &data.Record{
			Key:       i.Key(),
			Value:     i.Value(),
			Topic:     c.tp.Topic,
			Partition: c.tp.Partition,
		}
		records = append(records, record)
		i.Next()
	}

	return records
}

func (c *cache) Delete(record *data.Record) error {
	return c.backend.Delete(record.Key)
}

func (c *cache) decodeOffset(offset []byte) int64 {
	return int64(binary.LittleEndian.Uint64(offset))
}

func (c *cache) encodeOffset(offset int64) []byte {
	byt := make([]byte, 8)
	binary.LittleEndian.PutUint64(byt, uint64(offset))

	return byt
}

func (c *cache) LastSynced() (int64, error) {
	byt, err := c.offsetBackend.Get([]byte(c.offsetKeyPrefix()))
	if err != nil {
		return 0, err
	}

	if len(byt) < 1 {
		return 0, nil
	}

	return c.decodeOffset(byt), nil
}
