package store

import (
	"context"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/changelog"
	"github.com/tryfix/log"
)

type RecoverableStore interface {
	Store
	Recover(ctx context.Context) error
}

type recoverableStore struct {
	Store
	logger     log.Logger
	recovering bool
	topic      string
	changelog  changelog.Changelog
}

func (s *recoverableStore) Recover(ctx context.Context) error {

	s.logger.Info(
		fmt.Sprintf(`recovering from store [%s]...`, s.Name()))
	var c int
	records, err := s.changelog.ReadAll(ctx)
	if err != nil {
		return errors.WithPrevious(err,
			fmt.Sprintf(`cannot recover data for store [%s]`, s.Name()))
	}

	for _, record := range records {
		if err := s.Backend().Set(record.Key, record.Value, 0); err != nil {
			return err
		}
	}

	s.logger.Info(
		fmt.Sprintf(`[%d] records recovered for store [%s]...`, c, s.Name()))

	return nil
}

func (s *recoverableStore) String() string {
	return fmt.Sprintf("Backend: %s\nChangelogInfo: %s", s.Backend().Name(), s.changelog)
}
