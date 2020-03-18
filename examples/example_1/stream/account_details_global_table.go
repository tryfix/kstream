package stream

import (
	"github.com/tryfix/kstream/examples/example_1/encoders"
	kstream "github.com/tryfix/kstream/k-stream"
)

func initAccountDetailTable(builder *kstream.StreamBuilder) kstream.GlobalTable {

	return builder.GlobalTable(
		`account_detail`,
		encoders.KeyEncoder,
		encoders.AccountDetailsUpdatedEncoder,
		`account_detail_store`)
}