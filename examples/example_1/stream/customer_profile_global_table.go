package stream

import (
	"github.com/tryfix/kstream/examples/example_1/encoders"
	kstream "github.com/tryfix/kstream/k-stream"
)

func initCustomerProfileTable(builder *kstream.StreamBuilder) kstream.GlobalTable {

	return builder.GlobalTable(
		`customer_profile`,
		encoders.KeyEncoder,
		encoders.CustomerProfileUpdatedEncoder,
		`customer_profile_store`)
}