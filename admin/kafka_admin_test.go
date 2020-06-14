package admin

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/log"
	"testing"
)

func TestKafkaAdmin_FetchInfo(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetLeader("my_topic", 0, seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	saramaAdmin, err := sarama.NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topic := `my_topic`
	admin := &kafkaAdmin{
		admin:  saramaAdmin,
		logger: log.NewNoopLogger(),
	}
	tps, err := admin.FetchInfo([]string{topic})
	if err != nil {
		t.Error(err)
	}

	if tps[topic].NumPartitions != 1 {
		t.Fail()
	}

	err = saramaAdmin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestKafkaAdmin_CreateTopics(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"CreateTopicsRequest": sarama.NewMockCreateTopicsResponse(t),
	})

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	saramaAdmin, err := sarama.NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topic := `my_topic`
	admin := &kafkaAdmin{
		admin:  saramaAdmin,
		logger: log.NewNoopLogger(),
	}

	err = admin.CreateTopics(map[string]*Topic{
		topic: {
			Name:              topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = saramaAdmin.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestKafkaAdmin_DeleteTopics(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
		"DeleteTopicsRequest": sarama.NewMockDeleteTopicsResponse(t),
	})

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	saramaAdmin, err := sarama.NewClusterAdmin([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	topic := `my_topic`
	admin := &kafkaAdmin{
		admin:  saramaAdmin,
		logger: log.NewNoopLogger(),
	}

	_, err = admin.DeleteTopics([]string{topic})
	if err != nil {
		t.Fatal(err)
	}
	err = saramaAdmin.Close()
	if err != nil {
		t.Fatal(err)
	}
}
