package store

import (
	"context"
	"math/rand"
	"testing"
)

func BenchmarkDefaultStore_Set(b *testing.B) {

	store := makeTestStore(0)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.Set(ctx, rand.Intn(10000000), `100`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDefaultStore_Get(b *testing.B) {
	store := makeTestStore(0)
	ctx := context.Background()

	for i := 1; i < 999999; i++ {
		if err := store.Set(ctx, rand.Intn(i), `100`, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := store.Get(ctx, rand.Intn(999998)+1); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDefaultStore_Delete(b *testing.B) {
	store := makeTestStore(0)
	ctx := context.Background()

	for i := 1; i <= 999999; i++ {
		if err := store.Set(ctx, rand.Intn(i), `100`, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.Delete(ctx, rand.Intn(999998)+1); err != nil {
				b.Error(err)
			}
		}
	})
}
