package store

import (
	"context"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/kstream/encoding"
	"time"
)

type MockStore struct {
	name     string
	backend  backend.Backend
	kEncoder encoding.Encoder
	vEncoder encoding.Encoder
}

type MockRecord struct {
	ctx    context.Context
	key    interface{}
	value  interface{}
	expiry time.Duration
}

func NewMockStore(name string, kEncode encoding.Encoder, vEncoder encoding.Encoder, backend backend.Backend, records ...MockRecord) Store {
	store := &MockStore{
		name:     name,
		kEncoder: kEncode,
		vEncoder: vEncoder,
		backend:  backend,
	}

	for _, record := range records {
		if err := store.Set(record.ctx, record.key, record.value, record.expiry); err != nil {
			panic(err)
		}
	}

	return store
}

func (s *MockStore) Name() string {
	return s.name
}

func (s *MockStore) Backend() backend.Backend {
	return s.backend
}

func (s *MockStore) KeyEncoder() encoding.Encoder {
	return s.kEncoder
}

func (s *MockStore) ValEncoder() encoding.Encoder {
	return s.vEncoder
}

func (s *MockStore) Set(ctx context.Context, key interface{}, value interface{}, expiry time.Duration) error {
	k, err := s.kEncoder.Encode(key)
	if err != nil {
		return err
	}

	v, err := s.ValEncoder().Encode(value)
	if err != nil {
		return err
	}
	return s.backend.Set(k, v, expiry)
}

func (s *MockStore) Get(ctx context.Context, key interface{}) (value interface{}, err error) {
	k, err := s.kEncoder.Encode(key)
	if err != nil {
		return nil, err
	}

	v, err := s.backend.Get(k)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	val, err := s.vEncoder.Decode(v)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (*MockStore) GetRange(ctx context.Context, fromKey interface{}, toKey interface{}) (map[interface{}]interface{}, error) {
	panic("implement me")
}

func (*MockStore) GetAll(ctx context.Context) (Iterator, error) {
	panic("implement me")
}

func (s *MockStore) Delete(ctx context.Context, key interface{}) error {
	k, err := s.kEncoder.Encode(key)
	if err != nil {
		return err
	}

	return s.backend.Delete(k)
}

func (s *MockStore) String() string {
	return s.name
}
