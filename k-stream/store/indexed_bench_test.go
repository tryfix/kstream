package store

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkHashIndex_Write(b *testing.B) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := index.Write(strconv.Itoa(rand.Intn(100000)+1), `111,222`); err != nil {
				b.Error(err)
			}
		}

	})
}

func BenchmarkHashIndex_Read(b *testing.B) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	for i := 1; i < 1000; i++ {
		if err := index.Write(strconv.Itoa(i), `111,222`); err != nil {
			b.Error(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := index.Read(`111`); err != nil {
				b.Error(err)
			}
		}

	})
}
