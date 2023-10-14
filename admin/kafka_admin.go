/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

// Package admin provides an interface for kafka administrative operations
package admin

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/tryfix/errors"
	"github.com/tryfix/log"
)

type Partition struct {
	Id    int32
	Error error
}

type Topic struct {
	Name              string
	Partitions        []Partition
	Error             error
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	ConfigEntries     map[string]string
}

// KafkaAdmin
type KafkaAdmin interface {
	FetchInfo(topics []string) (map[string]*Topic, error)
	CreateTopics(topics map[string]*Topic) error
	DeleteTopics(topics []string) (map[string]error, error)
	Close()
}

type kafkaAdminOptions struct {
	BootstrapServers []string
	KafkaVersion     sarama.KafkaVersion
	Logger           log.Logger
}

func (opts *kafkaAdminOptions) apply(options ...KafkaAdminOption) {
	opts.KafkaVersion = sarama.V2_4_0_0
	opts.Logger = log.NewNoopLogger()
	for _, opt := range options {
		opt(opts)
	}
}

type KafkaAdminOption func(*kafkaAdminOptions)

func WithKafkaVersion(version sarama.KafkaVersion) KafkaAdminOption {
	return func(options *kafkaAdminOptions) {
		options.KafkaVersion = version
	}
}

func WithLogger(logger log.Logger) KafkaAdminOption {
	return func(options *kafkaAdminOptions) {
		options.Logger = logger
	}
}

type kafkaAdmin struct {
	admin  sarama.ClusterAdmin
	logger log.Logger
}

func NewKafkaAdmin(bootstrapServer []string, options ...KafkaAdminOption) *kafkaAdmin {
	opts := new(kafkaAdminOptions)
	opts.apply(options...)
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = opts.KafkaVersion
	logger := opts.Logger.NewLog(log.Prefixed(`kafka-admin`))
	admin, err := sarama.NewClusterAdmin(bootstrapServer, saramaConfig)
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot get controller - %+v`, err))
	}

	return &kafkaAdmin{
		admin:  admin,
		logger: logger,
	}
}

func (c *kafkaAdmin) FetchInfo(topics []string) (map[string]*Topic, error) {
	topicInfo := make(map[string]*Topic)
	topicMeta, err := c.admin.DescribeTopics(topics)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot get metadata : `)
	}

	for _, tp := range topicMeta {
		var pts []Partition
		for _, pt := range tp.Partitions {
			pts = append(pts, Partition{
				Id:    pt.ID,
				Error: pt.Err,
			})
		}
		topicInfo[tp.Name] = &Topic{
			Name:          tp.Name,
			Partitions:    pts,
			NumPartitions: int32(len(pts)),
		}
		if tp.Err != sarama.ErrNoError {
			topicInfo[tp.Name].Error = tp.Err
		}

		// configs
		confs, err := c.admin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        tp.Name,
			ConfigNames: []string{`cleanup.policy`, `min.insync.replicas`, `retention.ms`},
		})
		if err != nil {
			return nil, err
		}
		topicInfo[tp.Name].ConfigEntries = map[string]string{}
		for _, co := range confs {
			topicInfo[tp.Name].ConfigEntries[co.Name] = co.Value
		}
	}

	return topicInfo, nil
}

func (c *kafkaAdmin) CreateTopics(topics map[string]*Topic) error {
	for name, info := range topics {
		details := &sarama.TopicDetail{
			NumPartitions:     info.NumPartitions,
			ReplicationFactor: info.ReplicationFactor,
			ReplicaAssignment: info.ReplicaAssignment,
		}
		details.ConfigEntries = map[string]*string{}
		for cName, config := range info.ConfigEntries {
			configCpy := config
			details.ConfigEntries[cName] = &configCpy
		}

		err := c.admin.CreateTopic(name, details, false)
		if err != nil {
			if e, ok := err.(*sarama.TopicError); ok && (e.Err == sarama.ErrTopicAlreadyExists || e.Err == sarama.ErrNoError) {
				c.logger.Warn(err)
				continue
			}
			return errors.WithPrevious(err, `could not create topic`)
		}
	}

	c.logger.Info(`k-stream.kafkaAdmin`,
		fmt.Sprintf(`kafkaAdmin topics created - %+v`, topics))

	return nil
}

func (c *kafkaAdmin) DeleteTopics(topics []string) (map[string]error, error) {
	for _, topic := range topics {
		err := c.admin.DeleteTopic(topic)
		if err != nil {
			return nil, errors.WithPrevious(err, `could not delete topic :`)
		}
	}

	return make(map[string]error), nil
}

func (c *kafkaAdmin) Close() {
	if err := c.admin.Close(); err != nil {
		c.logger.Warn(`k-stream.kafkaAdmin`,
			fmt.Sprintf(`kafkaAdmin cannot close broker : %+v`, err))
	}
}
