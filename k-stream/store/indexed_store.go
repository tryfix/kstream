package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/tryfix/kstream/k-stream/encoding"
	"sync"
	"time"
)

type Index interface {
	Name() string
	Write(key, value interface{}) error
	Delete(val, value interface{}) error
	Read(index interface{}) ([]interface{}, error)
}

type IndexedStore interface {
	Store
	GetIndex(ctx context.Context, name string) (Index, error)
	GetIndexedRecords(ctx context.Context, index string, key interface{}) ([]interface{}, error)
}

type indexedStore struct {
	Store
	indexes map[string]Index
	mu      *sync.Mutex
}

func NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []Index, options ...Options) (IndexedStore, error) {
	store, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	idxs := make(map[string]Index)
	for _, idx := range indexes {
		idxs[idx.Name()] = idx
	}

	return &indexedStore{
		Store:   store,
		indexes: idxs,
		mu:      new(sync.Mutex),
	}, nil
}

func (i *indexedStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	// set indexes
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, index := range i.indexes {
		if err := index.Write(key.(string), val); err != nil {
			return err
		}
	}
	return i.Store.Set(ctx, key, val, expiry)
}

func (i *indexedStore) Delete(ctx context.Context, key interface{}) error {
	// delete indexes
	val, err := i.Store.Get(ctx, key)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	for _, index := range i.indexes {
		if err := index.Delete(key.(string), val); err != nil {
			return err
		}
	}
	return i.Store.Delete(ctx, key)
}

func (i *indexedStore) GetIndex(_ context.Context, name string) (Index, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	index, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}
	return index, nil
}

func (i *indexedStore) GetIndexedRecords(ctx context.Context, index string, key interface{}) ([]interface{}, error) {
	i.mu.Lock()
	idx, ok := i.indexes[index]
	i.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, index)
	}

	var records []interface{}
	indexes, err := idx.Read(key)
	if err != nil {
		if errors.Is(err, UnknownIndex) {
			return records, nil
		}
		return nil, err
	}

	for _, index := range indexes {
		record, err := i.Get(ctx, index)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}
