package store

import (
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/kstream/encoding"
)

type Iterator interface {
	SeekToFirst()
	SeekToLast()
	Seek(key interface{}) error
	Next()
	Prev()
	Close()
	Key() (interface{}, error)
	Value() (interface{}, error)
	Valid() bool
	Error() error
}

type iterator struct {
	iterator   backend.Iterator
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
}

func (i *iterator) SeekToFirst() {
	i.iterator.SeekToFirst()
}

func (i *iterator) SeekToLast() {
	i.iterator.SeekToLast()
}

func (i *iterator) Seek(key interface{}) error {
	k, err := i.keyEncoder.Encode(key)
	if err != nil {
		return err
	}

	i.iterator.Seek(k)
	return nil
}

func (i *iterator) Next() {
	i.iterator.Next()
}

func (i *iterator) Prev() {
	i.iterator.Prev()
}

func (i *iterator) Close() {
	i.iterator.Close()
}

func (i *iterator) Key() (interface{}, error) {
	k := i.iterator.Key()
	if len(k) < 1 {
		return nil, nil
	}

	return i.keyEncoder.Decode(k)
}

func (i *iterator) Value() (interface{}, error) {
	v := i.iterator.Value()
	if len(v) < 1 {
		return nil, nil
	}

	return i.valEncoder.Decode(v)
}

func (i *iterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *iterator) Error() error {
	return i.iterator.Error()
}
