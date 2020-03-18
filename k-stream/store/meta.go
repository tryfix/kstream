package store

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/log"
	"time"
)

type Meta struct {
	client       sarama.Client
	hostMappings map[string]string
	group        string
}

func NewMata(c sarama.Client, group string) *Meta {
	m := &Meta{
		client:       c,
		hostMappings: make(map[string]string),
		group:        group,
	}

	go m.runRefresher()
	return m
}

func (m *Meta) GetMeta(tp string) string {
	return m.hostMappings[tp]
}

func (m *Meta) Refresh() {

	b, err := m.client.Coordinator(m.group)
	if err != nil {
		log.Fatal(err)
	}

	res, err := b.DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: []string{m.group},
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, group := range res.Groups {
		if group.GroupId == m.group {
			for _, member := range group.Members {

				// get host port through following function
				//mt , _ := member.GetMemberMetadata()
				//mt.UserData

				ass, err := member.GetMemberAssignment()
				if err != nil {
					log.Fatal(err)
				}

				for topic, partitions := range ass.Topics {
					for _, p := range partitions {
						m.hostMappings[fmt.Sprintf(`%s_%d`, topic, p)] = member.ClientHost
					}
				}
			}
		}
	}

	log.Info(fmt.Sprintf(`host meta refreshed %+v`, m.hostMappings))
}

func (m *Meta) runRefresher() {
	t := time.NewTicker(30 * time.Second)

	for range t.C {
		m.Refresh()
	}
}
