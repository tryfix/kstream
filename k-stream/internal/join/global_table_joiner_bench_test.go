package join

import (
	"context"
	"testing"
)

func BenchmarkGlobalTableJoiner(b *testing.B) {
	ctx := context.Background()
	if err := testStore.Set(ctx, 200, rightRecord{
		PrimaryKey: 100,
		ForeignKey: 200,
	}, 0); err != nil {
		b.Error(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j := makeJoiner(InnerJoin)

			_, err := j.Join(ctx, 100, leftRecord{
				PrimaryKey: 100,
				ForeignKey: 200,
			})
			if err != nil {
				b.Error(err)
			}
		}
	})

}
