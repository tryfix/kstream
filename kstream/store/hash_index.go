package store

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type KeyMapper func(key, val interface{}) (idx string)

var UnknownIndex = errors.New(`index does not exist`)

type stringHashIndex struct {
	indexes map[string]map[interface{}]bool // indexKey:recordKey:bool
	mapper  KeyMapper
	mu      *sync.Mutex
	name    string
}

func NewStringHashIndex(name string, mapper KeyMapper) Index {
	return &stringHashIndex{
		indexes: make(map[string]map[interface{}]bool),
		mapper:  mapper,
		mu:      new(sync.Mutex),
		name:    name,
	}
}

func (s *stringHashIndex) String() string {
	return s.name
}

func (s *stringHashIndex) Write(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	_, ok := s.indexes[hashKey]
	if !ok {
		s.indexes[hashKey] = make(map[interface{}]bool)
	}
	s.indexes[hashKey][key] = true

	return nil
}

func (s *stringHashIndex) ValueIndexed(index, value interface{}) (bool, error) {
	hStr, ok := index.(string)
	if !ok {
		return false, errors.New(fmt.Sprintf(`unsupported hash type expected [string] given [%s]`, reflect.TypeOf(index)))
	}
	_, ok = s.indexes[hStr]
	if !ok {
		return false, nil
	}

	_, ok = s.indexes[hStr][value]
	return ok, nil
}

func (s *stringHashIndex) Hash(key, val interface{}) (hash interface{}) {
	return s.mapper(key, val)
}

func (s *stringHashIndex) WriteHash(hash, key interface{}) error {
	hStr, ok := hash.(string)
	if !ok {
		return errors.New(fmt.Sprintf(`unsupported hash type expected [string] given [%s]`, reflect.TypeOf(hash)))
	}
	_, ok = s.indexes[hStr]
	if !ok {
		s.indexes[hStr] = make(map[interface{}]bool)
	}
	s.indexes[hStr][key] = true

	return nil
}

func (s *stringHashIndex) Delete(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	if _, ok := s.indexes[hashKey]; !ok {
		return fmt.Errorf(`hashKey [%s] does not exist for [%s]`, hashKey, s.name)
	}

	delete(s.indexes[hashKey], key)
	return nil
}

func (s *stringHashIndex) Keys() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []interface{}

	for key := range s.indexes {
		keys = append(keys, key)
	}

	return keys
}

func (s *stringHashIndex) Values() map[interface{}][]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make(map[interface{}][]interface{})

	for idx, keys := range s.indexes {
		for key := range keys {
			values[idx] = append(values[idx], key)
		}
	}

	return values
}

func (s *stringHashIndex) Read(key interface{}) ([]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var indexes []interface{}
	index, ok := s.indexes[key.(string)]
	if !ok {
		return nil, UnknownIndex
	}
	for k := range index {
		indexes = append(indexes, k)
	}

	return indexes, nil
}
