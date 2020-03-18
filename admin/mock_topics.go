package admin

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/data"
	"sync"
)

type MockPartition struct {
	records []*data.Record
	*sync.Mutex
}

func (p *MockPartition) Append(r *data.Record) error {
	p.Lock()
	defer p.Unlock()
	if len(p.records) > 0 {
		r.Offset = int64(len(p.records))
	}

	//println(`appended`, r.Partition, r.Offset)
	p.records = append(p.records, r)

	return nil
}

func (p *MockPartition) Latest() int64 {
	p.Lock()
	defer p.Unlock()
	if len(p.records) < 1 {
		return 0
	}
	return p.records[len(p.records)-1].Offset
}

func (p *MockPartition) FetchAll() (records []*data.Record) {
	p.Lock()
	defer p.Unlock()
	return p.records
}

func (p *MockPartition) Fetch(start int64, limit int) (records []*data.Record, err error) {
	p.Lock()
	defer p.Unlock()

	if len(p.records) < 1 {
		return
	}

	if start == -1 /* latest offset */ {
		// get latest record
		start = int64(len(p.records))
	}

	if start == -2 /* oldest offset */ {
		start = 0
	}

	if start > int64(len(p.records)) {
		return
		//return nil, sarama.ErrOffsetOutOfRange
	}

	var from = start
	var to = limit

	if start > 0 {
		from = start
		to = int(start) + limit
	}

	if to > len(p.records) {
		to = len(p.records)
	}

	chunk := p.records[from:to]
	//println(`from`, from, `to`, to, `recs`, len(chunk), `ttt`, len(p.records))

	var count int
	for _, rec := range chunk {
		if count == limit {
			break
		}
		records = append(records, rec)
		count++
	}

	return
}

type MockTopic struct {
	Name       string
	partitions []*MockPartition
	Meta       *Topic
	mu         *sync.Mutex
}

func (tp *MockTopic) AddPartition(id int) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	tp.partitions[id] = &MockPartition{
		records: make([]*data.Record, 0),
		Mutex:   new(sync.Mutex),
	}

	return nil
}

func (tp *MockTopic) Partition(id int) (*MockPartition, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	return tp.partitions[id], nil
}

func (tp *MockTopic) Partitions() []*MockPartition {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	return tp.partitions
}

type Topics struct {
	*sync.Mutex
	topics map[string]*MockTopic
}

func NewMockTopics() *Topics {
	return &Topics{
		topics: make(map[string]*MockTopic),
		Mutex:  new(sync.Mutex),
	}
}

func (td *Topics) AddTopic(topic *MockTopic) error {
	td.Lock()
	defer td.Unlock()
	_, ok := td.topics[topic.Name]
	if ok {
		return errors.New(`topic already exists`)
	}
	topic.mu = new(sync.Mutex)
	topic.partitions = make([]*MockPartition, topic.Meta.NumPartitions)
	for i := int32(0); i < topic.Meta.NumPartitions; i++ {
		topic.Meta.Partitions = append(topic.Meta.Partitions, Partition{
			Id:    i,
			Error: nil,
		})
		if err := topic.AddPartition(int(i)); err != nil {
			return err
		}
	}
	td.topics[topic.Name] = topic
	return nil
}

func (td *Topics) RemoveTopic(name string) error {
	td.Lock()
	defer td.Unlock()
	_, ok := td.topics[name]
	if ok {
		return errors.New(`topic does not exists`)
	}
	delete(td.topics, name)
	return nil
}

func (td *Topics) Topic(name string) (*MockTopic, error) {
	td.Lock()
	defer td.Unlock()

	t, ok := td.topics[name]
	if !ok {
		return t, sarama.ErrUnknownTopicOrPartition
	}

	return t, nil
}

func (td *Topics) Topics() map[string]*MockTopic {
	td.Lock()
	defer td.Unlock()

	return td.topics
}

func (tp *MockTopic) FetchAll() (records []*data.Record) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	rec := make([]*data.Record, 0)
	for _, pt := range tp.partitions {
		rec = append(rec, pt.FetchAll()...)
	}
	return rec
}
