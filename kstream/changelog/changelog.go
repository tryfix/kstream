/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package changelog

import (
	"context"
	"github.com/tryfix/kstream/data"
)

type Builder func(id string, topic string, partition int32, opts ...Options) (Changelog, error)

type Changelog interface {
	ReadAll(ctx context.Context) ([]*data.Record, error)
	Put(ctx context.Context, record *data.Record) error
	PutAll(ctx context.Context, record []*data.Record) error
	Delete(ctx context.Context, record *data.Record) error
	DeleteAll(ctx context.Context, record []*data.Record) error
	Close()
}
