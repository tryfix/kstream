package stream

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tryfix/kstream/examples/example_1/events"
	"github.com/tryfix/kstream/kstream"
	"github.com/tryfix/kstream/kstream/branch"
	"github.com/tryfix/kstream/kstream/encoding"
	"time"
)

type AccountDebited struct {
	Upstream             kstream.Stream
	AccountDetailTable   kstream.GlobalTable
	CustomerProfileTable kstream.GlobalTable
	KeyEncoder           func() encoding.Encoder
	MessageEncoder       func() encoding.Encoder
}

func (ad AccountDebited) Init() {
	accountDebitedBranches := ad.Upstream.Branch([]branch.Details{{Name: `account_debited`, Predicate: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
		_, ok := val.(events.AccountDebited)
		return ok, nil
	}}})

	accountDebitedBranch := accountDebitedBranches[0]

	filteredAccountDebited := accountDebitedBranch.Filter(ad.filterFromTimestamp)

	joinedDebitedAccountDetails := filteredAccountDebited.JoinGlobalTable(ad.AccountDetailTable, ad.accountDebitedAccountDetailsKeyMapping, ad.accountDebitedAccountDetailsMapping, 1) //1 for inner join

	joinedDebitedCustomerProfile := joinedDebitedAccountDetails.JoinGlobalTable(ad.CustomerProfileTable, ad.accountDebitedMessageCustomerProfileKeyMapping, ad.accountMessageCustomerProfileDetailsMapping, 1)

	joinedDebitedCustomerProfile.To(`message`, ad.KeyEncoder, ad.MessageEncoder)
}

func (ad AccountDebited) filterFromTimestamp(ctx context.Context, key, value interface{}) (b bool, e error) {

	accDebited, _ := value.(events.AccountDebited)
	if time.Now().UnixNano()/1e6-accDebited.Timestamp > 300000 {
		return false, nil
	}

	return true, nil
}

func (ad AccountDebited) accountDebitedAccountDetailsKeyMapping(key interface{}, value interface{}) (mappedKey interface{}, err error) {

	accDebited, _ := value.(events.AccountDebited)

	return accDebited.Body.AccountNo, nil
}

func (ad AccountDebited) accountDebitedAccountDetailsMapping(left interface{}, right interface{}) (joined interface{}, err error) {

	l, _ := left.(events.AccountDebited)
	r, _ := right.(events.AccountDetailsUpdated)

	dateTime := time.Unix(l.Body.DebitedAt, 0).Format(time.RFC1123)
	text := fmt.Sprintf(`Your a/c %d is debited with %v USD on %v at %v`, l.Body.AccountNo, l.Body.Amount, dateTime, l.Body.Location)

	message := events.MessageCreated{
		ID:        uuid.New().String(),
		Type:      "message_created",
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	message.Body.CustomerID = r.Body.CustomerID
	message.Body.Text = text

	return message, nil
}

func (ad AccountDebited) accountDebitedMessageCustomerProfileKeyMapping(key interface{}, value interface{}) (mappedKey interface{}, err error) {

	message, _ := value.(events.MessageCreated)

	return message.Body.CustomerID, nil
}

func (ad AccountDebited) accountMessageCustomerProfileDetailsMapping(left interface{}, right interface{}) (joined interface{}, err error) {

	l, _ := left.(events.MessageCreated)
	r, _ := right.(events.CustomerProfileUpdated)

	l.Body.Address = r.Body.ContactDetails.Address
	l.Body.Phone = r.Body.ContactDetails.Phone
	l.Body.Email = r.Body.ContactDetails.Email

	return l, nil
}
