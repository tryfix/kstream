package kstream

import (
	"context"
	"fmt"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/log"
)

type reBalanceHandler struct {
	userHandler     consumer.ReBalanceHandler
	processors      *processorPool
	logger          log.Logger
	builder         *StreamBuilder
	rebalancedCount int
}

func (s *reBalanceHandler) OnPartitionRevoked(ctx context.Context, revoked []consumer.TopicPartition) error {
	s.logger.Info(fmt.Sprintf(`partitions %v revoking...`, revoked))
	defer s.logger.Info(fmt.Sprintf(`partitions %v revoked`, revoked))
	for _, tp := range revoked {
		s.processors.Processor(tp).Stop()
	}

	if s.userHandler != nil {
		return s.userHandler.OnPartitionRevoked(ctx, revoked)
	}

	if err := s.startChangelogReplicas(revoked); err != nil {
		return err
	}

	return nil
}

func (s *reBalanceHandler) OnPartitionAssigned(ctx context.Context, assigned []consumer.TopicPartition) error {

	s.logger.Info(fmt.Sprintf(`partitions %v assigning...`, assigned))
	defer s.logger.Info(fmt.Sprintf(`partitions %v assigned`, assigned))

	for _, tp := range assigned {
		if err := s.processors.addProcessor(tp); err != nil {
			return err
		}

		if err := s.processors.Processor(tp).boot(); err != nil {
			return err
		}

		if err := s.stopChangelogReplicas(assigned); err != nil {
			return err
		}
	}

	s.logger.Info(`streams assigned`)
	if s.userHandler != nil {
		return s.userHandler.OnPartitionAssigned(ctx, assigned)
	}
	s.rebalancedCount++
	return nil
}

func (s *reBalanceHandler) stopChangelogReplicas(allocated []consumer.TopicPartition) error {
	if len(allocated) > 0 && s.rebalancedCount > 0 {
		for _, tp := range allocated {
			// stop started replicas
			if s.builder.streams[tp.Topic].config.changelog.replicated {
				if err := s.builder.changelogReplicaManager.StopReplicas([]consumer.TopicPartition{
					{Topic: s.builder.streams[tp.Topic].config.changelog.topic.Name, Partition: tp.Partition},
				}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *reBalanceHandler) startChangelogReplicas(allocated []consumer.TopicPartition) error {
	if len(allocated) > 0 {
		for _, tp := range allocated {
			// stop started replicas
			if s.builder.streams[tp.Topic].config.changelog.replicated {
				if err := s.builder.changelogReplicaManager.StartReplicas([]consumer.TopicPartition{
					{Topic: s.builder.streams[tp.Topic].config.changelog.topic.Name, Partition: tp.Partition},
				}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
