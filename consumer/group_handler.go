package consumer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

type ReBalanceHandler interface {
	OnPartitionRevoked(ctx context.Context, revoked []TopicPartition) error
	OnPartitionAssigned(ctx context.Context, assigned []TopicPartition) error
}

type groupHandler struct {
	reBalanceHandler      ReBalanceHandler
	partitionMap          map[string]*partition
	partitions            chan Partition
	logger                log.Logger
	recordUuidExtractFunc RecordUuidExtractFunc
	mu                    *sync.Mutex
	metrics               struct {
		reporter         metrics.Reporter
		reBalancing      metrics.Gauge
		commitLatency    metrics.Observer
		reBalanceLatency metrics.Observer
		endToEndLatency  metrics.Observer
	}
}

func (h *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	tps := h.extractTps(session.Claims())
	h.logger.Info(fmt.Sprintf(`setting up partitions [%#v]`, tps))
	if err := h.reBalanceHandler.OnPartitionAssigned(session.Context(), tps); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, tp := range tps {
		p := newPartition(tp)
		h.partitionMap[tp.String()] = p
		h.partitions <- p
	}

	return nil
}

func (h *groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	tps := h.extractTps(session.Claims())
	h.logger.Info(fmt.Sprintf(`cleaning up partitions [%#v]`, tps))

	h.mu.Lock()
	for _, tp := range tps {
		h.partitionMap[tp.String()].close()
		delete(h.partitionMap, tp.String())
	}
	h.mu.Unlock()

	return h.reBalanceHandler.OnPartitionRevoked(session.Context(), tps)
}

func (h *groupHandler) ConsumeClaim(g sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	tp := TopicPartition{
		Topic:     c.Topic(),
		Partition: c.Partition(),
	}

	h.mu.Lock()
	h.partitionMap[tp.String()].groupSession = g
	ch := h.partitionMap[tp.String()].records
	h.mu.Unlock()

	for msg := range c.Messages() {
		t := time.Since(msg.Timestamp)
		h.metrics.endToEndLatency.Observe(float64(t.Nanoseconds()/1e3), map[string]string{
			`topic`:     msg.Topic,
			`partition`: fmt.Sprint(msg.Partition),
		})

		record := &data.Record{
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Timestamp: msg.Timestamp,
			Headers:   data.RecordHeaders(msg.Headers),
		}

		uuid := h.recordUuidExtractFunc(record)
		record.UUID = uuid

		h.logger.Trace("record received after " +
			t.String() +
			" for " + tp.String() +
			" with key: " + string(msg.Key) +
			" and value: " + string(msg.Value) +
			" with record-id [" + record.UUID.String() + "]")

		ch <- record
	}

	return nil
}

func (h *groupHandler) extractTps(kafkaTps map[string][]int32) []TopicPartition {
	tps := make([]TopicPartition, 0)
	for topic, partitions := range kafkaTps {
		for _, p := range partitions {
			tps = append(tps, TopicPartition{
				Topic:     topic,
				Partition: p,
			})
		}
	}
	return tps
}
