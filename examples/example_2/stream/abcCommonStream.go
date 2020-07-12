package stream

import (
	"github.com/tryfix/kstream/examples/example_2/domain"
	"github.com/tryfix/kstream/examples/example_2/encoders"
	"github.com/tryfix/kstream/kstream"
)

func initCommonStream(builder *kstream.StreamBuilder) kstream.Stream {
	str := builder.Stream(
		domain.ABCTopic,
		encoders.StringEncoder,
		encoders.CommonABEncoder,
		kstream.WithConfig(map[string]interface{}{
			//`stream.processor.retry`:          2,
			//`stream.processor.retry.interval`: 3000,
			//`stream.processor.changelog`: false,
			//`stream.processor.changelog.minInSyncReplicas`: 2,
			//`stream.processor.changelog.replicationFactor`: 3,
			//`stream.processor.changelog.buffered`: true,
			//`stream.processor.changelog.BufferedSize`: 100,
		}))

	AStream{
		Upstream: str,
	}.Init()

	return str
}
