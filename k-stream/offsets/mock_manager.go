package offsets

import "github.com/tryfix/kstream/admin"

type MockManager struct {
	Topics *admin.Topics
}

func (m *MockManager) OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error) {
	oldest, err := m.GetOffsetOldest(topic, partition)
	if err != nil {
		return
	}

	latest, err := m.GetOffsetLatest(topic, partition)
	if err != nil {
		return
	}
	return offsetValid(offset, oldest, latest), nil
}

func (m *MockManager) GetOffsetLatest(topic string, partition int32) (offset int64, err error) {
	tp, err := m.Topics.Topic(topic)
	if err != nil {
		return
	}

	pt, err := tp.Partition(int(partition))
	if err != nil {
		return
	}

	return pt.Latest(), nil
}

func (m *MockManager) GetOffsetOldest(topic string, partition int32) (offset int64, err error) {
	return 0, nil
}

func (m *MockManager) Close() error {
	return nil
}
