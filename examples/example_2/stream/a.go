package stream

import (
	"context"
	"fmt"
	"github.com/tryfix/kstream/examples/example_2/events"
	kstream "github.com/tryfix/kstream/kstream"
	"github.com/tryfix/kstream/kstream/branch"
	"github.com/tryfix/log"
	"time"
)

type AStream struct {
	Upstream kstream.Stream
}

func (ac AStream) Init() {
	branches := ac.Upstream.Branch([]branch.Details{
		{
			Name: `a_branch`,
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
				_, ok := val.(events.AA)
				return ok, nil
			},
		},
		{
			Name: `b_branch`,
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
				_, ok := val.(events.BB)
				return ok, nil
			},
		},
		{
			Name: `c_branch`,
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
				_, ok := val.(events.CC)
				return ok, nil
			},
		},
	})

	filteredAStream := branches[0].
		Filter(ac.filterAFromTimestamp).
		Process(func(ctx context.Context, key, value interface{}) error {
			a := value.(events.AA)
			log.Info(fmt.Sprintf(`a stream received with key %v, and value %+v`, key, a))
			return nil
		})
	filteredBStream := branches[1].
		Filter(ac.filterBFromTimestamp).
		Process(func(ctx context.Context, key, value interface{}) error {
			b := value.(events.BB)
			log.Info(fmt.Sprintf(`b stream received with key %v, and value %+v`, key, b))
			return nil
		})

	filteredCStream := branches[2].
		Filter(ac.filterCFromTimestamp).
		Process(func(ctx context.Context, key, value interface{}) error {
			c := value.(events.CC)
			log.Info(fmt.Sprintf(`c stream received with key %v, and value %+v`, key, c))
			return nil
		})

	ABJoinedStream := filteredAStream.JoinStream(filteredBStream, func(left, right interface{}) (joined interface{}, err error) {
		a := left.(events.AA)
		b := right.(events.BB)
		return events.AB{
			ID:         a.ID,
			Type:       "AB",
			AAA:        a.AAA,
			BBB:        b.BBB,
			TimestampA: a.Timestamp,
			TimestampB: b.Timestamp,
		}, nil
	})

	ABJoinedProcessedStream := ABJoinedStream.Process(func(ctx context.Context, key, value interface{}) error {
		ab := value.(events.AB)
		log.Info(fmt.Sprintf(`joined ab received with key %v, and value %+v`, key, ab))
		return nil
	})

	ABCJoinedStream := ABJoinedProcessedStream.JoinStream(filteredCStream, func(left, right interface{}) (joined interface{}, err error) {
		ab := left.(events.AB)
		c := right.(events.CC)
		return events.ABC{
			ID:         ab.ID,
			Type:       "ABC",
			AAA:        ab.AAA,
			BBB:        ab.BBB,
			CCC:        c.CCC,
			TimestampA: ab.TimestampA,
			TimestampB: ab.TimestampB,
			TimestampC: c.Timestamp,
		}, nil
	})

	ABCJoinedStream.Process(func(ctx context.Context, key, value interface{}) error {
		abc := value.(events.ABC)
		log.Info(fmt.Sprintf(`joined abc received with key %v, and value %+v`, key, abc))
		return nil
	})
}

func (ac AStream) filterAFromTimestamp(ctx context.Context, key, value interface{}) (b bool, e error) {

	accCredited, _ := value.(events.AA)
	if time.Now().UnixNano()/1e6-accCredited.Timestamp > 300000 {
		return false, nil
	}

	return true, nil
}

func (ac AStream) filterBFromTimestamp(ctx context.Context, key, value interface{}) (b bool, e error) {

	accCredited, _ := value.(events.BB)
	if time.Now().UnixNano()/1e6-accCredited.Timestamp > 300000 {
		return false, nil
	}

	return true, nil
}

func (ac AStream) filterCFromTimestamp(ctx context.Context, key, value interface{}) (b bool, e error) {

	accCredited, _ := value.(events.CC)
	if time.Now().UnixNano()/1e6-accCredited.Timestamp > 300000 {
		return false, nil
	}

	return true, nil
}
