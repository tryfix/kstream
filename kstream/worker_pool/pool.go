package worker_pool

import (
	"context"
	"fmt"
	"github.com/tryfix/kstream/kstream/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

type ExecutionOrder int

const (
	OrderRandom ExecutionOrder = iota
	OrderByKey
	OrderPreserved
)

func (eo ExecutionOrder) String() string {
	order := `OrderRandom`

	if eo == OrderByKey {
		return `OrderByKey`
	}

	if eo == OrderPreserved {
		return `OrderPreserved`
	}

	return order
}

type task struct {
	ctx     context.Context
	key     []byte
	val     []byte
	doneClb func()
}

type PoolConfig struct {
	NumOfWorkers     int
	WorkerBufferSize int
	Order            ExecutionOrder
}

type Pool struct {
	id       string
	topology *topology.TopologyBuilder
	size     int64
	workers  []*worker
	logger   log.Logger
	order    ExecutionOrder
	stopped  chan bool
	hasher   hash.Hash32
}

func NewPool(id string, tb *topology.TopologyBuilder, metricsReporter metrics.Reporter, logger log.Logger, config *PoolConfig) *Pool {
	p := &Pool{
		id:       id,
		topology: tb,
		size:     int64(config.NumOfWorkers),
		order:    config.Order,
		logger:   logger.NewLog(log.Prefixed(`pool`)),
		workers:  make([]*worker, config.NumOfWorkers),
		hasher:   fnv.New32a(),
		stopped:  make(chan bool, 1),
	}

	bufferUsage := metricsReporter.Counter(metrics.MetricConf{
		Path:   `k_stream_task_pool_worker_buffer`,
		Labels: []string{`pool_id`},
	})

	for i := int64(config.NumOfWorkers) - 1; i >= 0; i-- {
		t, err := tb.Build()
		if err != nil {
			p.logger.Fatal(`k-stream.streamProcessor`, err)
		}

		w := &worker{
			topology:    t,
			pool:        p,
			logger:      p.logger.NewLog(log.Prefixed(fmt.Sprintf(`worker-%d`, i))),
			tasks:       make(chan task, config.WorkerBufferSize),
			bufferUsage: bufferUsage,
		}

		p.workers[i] = w
	}

	for _, w := range p.workers {
		go w.start()
	}

	return p
}

func (p *Pool) Run(ctx context.Context, key, val []byte, doneClb func()) {
	w, err := p.worker(key)
	if err != nil {
		p.logger.ErrorContext(ctx, `k-stream.task_pool`, err)
		return
	}

	w.tasks <- task{
		key:     key,
		ctx:     ctx,
		val:     val,
		doneClb: doneClb,
	}

	/*select {
	case w.tasks <- t:
		break
	default:
		p.logger.Debug(`k-stream.task_pool`, `worker buffer full`)
		break
	}*/
}

func (p *Pool) Stop() {
	for _, w := range p.workers {
		w.stop()
	}
}

func (p *Pool) worker(key []byte) (*worker, error) {
	var worker *worker
	var w int64
	if p.order == OrderRandom && p.size > 1 {
		w = int64(rand.Int63n(p.size - 1))
		return p.workers[w], nil
	}

	if p.order == OrderByKey {
		p.hasher.Reset()
		_, err := p.hasher.Write(key)
		if err != nil {
			return worker, err
		}

		w = int64(p.hasher.Sum32()) % p.size
	}

	return p.workers[w], nil
}

type worker struct {
	topology    topology.Topology
	tasks       chan task
	pool        *Pool
	logger      log.Logger
	bufferUsage metrics.Counter
}

func (w *worker) start() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			w.bufferUsage.Count((float64(len(w.tasks))/float64(cap(w.tasks)))*100, map[string]string{`pool_id`: w.pool.id})
		}
	}()

	for task := range w.tasks {
		_, _, err := w.topology.Run(task.ctx, task.key, task.val)
		if err != nil {
			w.logger.ErrorContext(task.ctx, fmt.Sprintf(`k-stream.task_pool error due to %s`, err))
		}
		task.doneClb()
	}
}

func (w *worker) stop() {
	close(w.tasks)
	w.pool = nil
	w.bufferUsage = nil
}
