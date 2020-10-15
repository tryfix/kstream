package encoders

import (
	"github.com/tryfix/kstream/examples/example_2/events"
	"github.com/tryfix/kstream/kstream/encoding"
)

//var KeyEncoder = func() encoding.Encoder { return Int64Encoder{} }
var StringEncoder = func() encoding.Encoder { return encoding.StringEncoder{} }

var CommonABEncoder = func() encoding.Encoder { return CommonEncoder{} }

var AAEncoder = func() encoding.Encoder { return events.AA{} }
var BBEncoder = func() encoding.Encoder { return events.BB{} }
var CCEncoder = func() encoding.Encoder { return events.CC{} }
