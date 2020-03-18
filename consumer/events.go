/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package consumer

import "fmt"

type Event interface {
	String() string
}

type PartitionAllocated struct {
	tps []TopicPartition
}

func (p *PartitionAllocated) String() string {
	return fmt.Sprintf(`%v`, p.tps)
}

func (p *PartitionAllocated) TopicPartitions() []TopicPartition {
	return p.tps
}

type PartitionRemoved struct {
	tps []TopicPartition
}

func (p *PartitionRemoved) String() string {
	return fmt.Sprintf(`%v`, p.tps)
}

func (p *PartitionRemoved) TopicPartitions() []TopicPartition {
	return p.tps
}

type PartitionEnd struct {
	tps []TopicPartition
}

func (p *PartitionEnd) String() string {
	return fmt.Sprintf(`%v`, p.tps)
}

func (p *PartitionEnd) TopicPartitions() []TopicPartition {
	return p.tps
}

type Error struct {
	err error
}

func (p *Error) String() string {
	return fmt.Sprint(`consumer error`, p.err)
}

func (p *Error) Error() string {
	return fmt.Sprint(`consumer error`, p.err)
}
