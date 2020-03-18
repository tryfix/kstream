package changelog

import (
	"github.com/tryfix/kstream/producer"
	"time"
)

type options struct {
	buffered      bool
	bufferSize    int
	flushInterval time.Duration
	producer      producer.Producer
}

type Options func(config *options)

func (c *options) apply(id string, options ...Options) error {

	if err := c.applyDefaults(id); err != nil {
		return err
	}

	for _, opt := range options {
		opt(c)
	}

	return nil
}

func (c *options) applyDefaults(id string) error {
	return nil
}

func Producer(p producer.Producer) Options {
	return func(config *options) {
		config.producer = p
	}
}

func Buffered(size int) Options {
	return func(config *options) {
		config.buffered = true
		config.bufferSize = size
	}
}

func FlushInterval(d time.Duration) Options {
	return func(config *options) {
		config.flushInterval = d
	}
}
