package worker_pool

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

var pOrdered *Pool
var pRandom *Pool

func init() {
	//builder := node.NewMockTopologyBuilder(10, 10)
	//
	//pl := NewPool(`1`, builder, metrics.NoopReporter(), &PoolConfig{
	//	NumOfWorkers:     1000,
	//	Order:            OrderByKey,
	//	WorkerBufferSize: 10000,
	//})
	//pOrdered = pl
	//
	//plRd := NewPool(`2`, builder, metrics.NoopReporter(), &PoolConfig{
	//	NumOfWorkers:     1000,
	//	Order:            OrderRandom,
	//	WorkerBufferSize: 10000,
	//})
	//pRandom = plRd
}

func BenchmarkPool_Run_Random(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		k := rand.Intn(8) + 1
		for pb.Next() {
			pRandom.Run(context.Background(), []byte(`200`), []byte(fmt.Sprintf(`br_%d`, k)), func() {})
		}
	})

}

func BenchmarkPool_Run_Ordered(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		k := rand.Intn(8) + 1
		for pb.Next() {
			pOrdered.Run(context.Background(), []byte(`200`), []byte(fmt.Sprintf(`br_%d`, k)), func() {})
		}
	})

}

//func BenchmarkPool_Run_Ordered(b *testing.B) {
//
//	var count int
//
//	f := new(flow.MockFlow)
//	var processor processors.ProcessFunc = func(ctx context.Context, key interface{}, value interface{}) error {
//
//		v, ok := key.(int)
//		if !ok {
//			b.Error(fmt.Sprintf(`expected [int] have [%+v]`, reflect.TypeOf(key)))
//		}
//
//		count = v
//
//		return nil
//	}
//
//	f.ProcessorsM = append(f.ProcessorsM, processor)
//	executor := flow.NewPlowExecutor(f, logger.DefaultLogger)
//
//	p := NewPool(executor, &encoding.IntEncoder{}, &encoding.IntEncoder{}, &PoolConfig{
//		NumOfWorkers:     20,
//		Order:            OrderByKey,
//		WorkerBufferSize: 10,
//	})
//
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			done := p.Run(context.Background(), []byte(string(`100`)), []byte(string(`100`)))
//			<-done
//		}
//	})
//
//	if count != 100 {
//		b.Fail()
//	}
//
//}
