package offsets

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/log"
)

type Manager interface {
	OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error)
	GetOffsetLatest(topic string, partition int32) (offset int64, err error)
	GetOffsetOldest(topic string, partition int32) (offset int64, err error)
	Close() error
}

type Config struct {
	Config           *sarama.Config
	BootstrapServers []string
	Logger           log.Logger
}

type manager struct {
	client sarama.Client
}

func NewManager(config *Config) Manager {
	logger := config.Logger.NewLog(log.Prefixed(`offset-manager`))
	client, err := sarama.NewClient(config.BootstrapServers, config.Config)
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot initiate builder deu to [%+v]`, err))
	}
	return &manager{client: client}
}

func (m *manager) OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error) {
	isValid, _, err = m.validate(topic, partition, offset)
	return
}

func (m *manager) GetOffsetLatest(topic string, partition int32) (offset int64, err error) {
	partitionStart, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return offset, fmt.Errorf(`cannot get latest offset for %s-%d due to %w`, topic, partition, err)
	}

	return partitionStart, nil
}

func (m *manager) GetOffsetOldest(topic string, partition int32) (offset int64, err error) {
	partitionStart, err := m.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return offset, fmt.Errorf(`cannot get oldes offset for %s-%d due to %w`, topic, partition, err)
	}

	return partitionStart, nil
}

func (m *manager) Close() error {
	return m.client.Close()
}

func offsetValid(offset, bkStart, bkEnd int64) bool {
	return offset >= bkStart && offset < bkEnd
}

func (m *manager) validate(topic string, partition int32, offset int64) (isValid bool, valid int64, err error) {

	startOffset, err := m.GetOffsetLatest(topic, partition)
	if err != nil {
		return false, 0, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	endOffset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return false, 0, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	return offsetValid(offset, startOffset, endOffset), startOffset, nil
}
