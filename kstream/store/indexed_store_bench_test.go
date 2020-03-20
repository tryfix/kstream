package store

import (
	"context"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/backend/memory"
	"github.com/tryfix/kstream/kstream/encoding"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func BenchmarkIndexedStore_Set(b *testing.B) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &indexedStore{
		Store:   NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)),
		indexes: map[string]Index{`foo`: index},
		mu:      new(sync.Mutex),
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := i.Set(context.Background(), strconv.Itoa(rand.Intn(99999)+1), `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkIndexedStore_GetIndexedRecords(b *testing.B) {
	indexedStore := NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0))
	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := indexedStore.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `:`)[0]
	})

	st, err := NewIndexedStore(
		`foo`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		[]Index{index},
		WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
	if err != nil {
		b.Error(err)
	}

	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := st.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := st.GetIndexedRecords(context.Background(), `foo`, strconv.Itoa(rand.Intn(4)+1)); err != nil {
				b.Error(err)
			}
		}
	})
}
