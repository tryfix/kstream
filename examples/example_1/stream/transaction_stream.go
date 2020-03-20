package stream

import (
	"github.com/tryfix/kstream/examples/example_1/encoders"
	kstream "github.com/tryfix/kstream/kstream"
)

func initTransactionStream(builder *kstream.StreamBuilder) kstream.Stream {
	return builder.Stream(
		`transaction`,
		encoders.KeyEncoder,
		encoders.TransactionReceivedEncoder,
		kstream.WithConfig(map[string]interface{}{
			//`stream.processor.retry`:          2,
			//`stream.processor.retry.interval`: 3000,
			//`stream.processor.changelog`: false,
			//`stream.processor.changelog.minInSyncReplicas`: 2,
			//`stream.processor.changelog.replicationFactor`: 3,
			//`stream.processor.changelog.buffered`: true,
			//`stream.processor.changelog.BufferedSize`: 100,
		}))
}
