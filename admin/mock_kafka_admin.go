/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package admin

type MockKafkaAdmin struct {
	Topics *Topics
}

func NewMockAdminWithTopics(tps map[string]*Topic) *MockKafkaAdmin {
	topics := NewMockTopics()
	admin := &MockKafkaAdmin{Topics: topics}
	admin.CreateTopics(tps)
	return admin
}

func (m *MockKafkaAdmin) FetchInfo(topics []string) (map[string]*Topic, error) {
	tps := make(map[string]*Topic)
	for _, topic := range topics {
		info, err := m.Topics.Topic(topic)
		if err != nil {
			return nil, err
		}
		tps[topic] = info.Meta
	}

	return tps, nil
}

func (m *MockKafkaAdmin) CreateTopics(topics map[string]*Topic) error {
	for name, topic := range topics {
		if err := m.createTopic(name, topic); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockKafkaAdmin) createTopic(name string, info *Topic) error {
	topic := &MockTopic{
		Name: name,
		Meta: info,
	}

	err := m.Topics.AddTopic(topic)
	if err != nil {
		return err
	}

	return nil
}

func (m *MockKafkaAdmin) DeleteTopics(topics []string) (map[string]error, error) {
	for _, tp := range topics {
		if err := m.Topics.RemoveTopic(tp); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (m *MockKafkaAdmin) Close() {}
