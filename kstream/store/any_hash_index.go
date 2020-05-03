package store

import (
	"fmt"
	"sync"
)

type anyHashIndex struct {
	indexes map[interface{}]map[interface{}]bool // indexKey:recordKey:bool
	mapper  KeyMapper
	mu      *sync.Mutex
	name    string
}

func NewAnyHashIndex(name string, mapper KeyMapper) Index {
	return &anyHashIndex{
		indexes: make(map[interface{}]map[interface{}]bool),
		mapper:  mapper,
		mu:      new(sync.Mutex),
		name:    name,
	}
}

func (s *anyHashIndex) Name() string {
	return s.name
}

func (s *anyHashIndex) Write(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	_, ok := s.indexes[hashKey]
	if !ok {
		s.indexes[hashKey] = make(map[interface{}]bool)
	}
	s.indexes[hashKey][key.(string)] = true
	return nil
}

func (s *anyHashIndex) Delete(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	if _, ok := s.indexes[hashKey]; !ok {
		return fmt.Errorf(`hashKey %s does not exist for %s`, hashKey, s.name)
	}

	delete(s.indexes[hashKey], key.(string))
	return nil
}

func (s *anyHashIndex) Read(key interface{}) ([]interface{}, error) {
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
