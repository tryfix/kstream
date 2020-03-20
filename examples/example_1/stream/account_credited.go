package stream

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tryfix/kstream/examples/example_1/events"
	kstream "github.com/tryfix/kstream/kstream"
	"github.com/tryfix/kstream/kstream/branch"
	"github.com/tryfix/kstream/kstream/encoding"
	"time"
)

type AccountCredited struct {
	Upstream             kstream.Stream
	AccountDetailTable   kstream.GlobalTable
	CustomerProfileTable kstream.GlobalTable
	KeyEncoder           func() encoding.Encoder
	MessageEncoder       func() encoding.Encoder
}

func (ac AccountCredited) Init() {
	accountCreditedBranches := ac.Upstream.Branch([]branch.Details{
		{
			Name: `account_credited`,
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
				_, ok := val.(events.AccountCredited)
				return ok, nil
			},
		}})

	accountCreditedBranch := accountCreditedBranches[0]

	filteredAccountCredited := accountCreditedBranch.Filter(ac.filterFromTimestamp)

	joinedCreditedAccountDetails := filteredAccountCredited.JoinGlobalTable(ac.AccountDetailTable, ac.accountCreditedAccountDetailsKeyMapping, ac.accountCreditedAccountDetailsMapping, 1) //1 for inner join

	joinedCreditedCustomerProfile := joinedCreditedAccountDetails.JoinGlobalTable(ac.CustomerProfileTable, ac.accountCreditedMessageCustomerProfileKeyMapping, ac.accountMessageCustomerProfileDetailsMapping, 1)

	joinedCreditedCustomerProfile.To(`message`, ac.KeyEncoder, ac.MessageEncoder)
}

func (ac AccountCredited) filterFromTimestamp(ctx context.Context, key, value interface{}) (b bool, e error) {

	accCredited, _ := value.(events.AccountCredited)
	if time.Now().UnixNano()/1e6-accCredited.Timestamp > 300000 {
		return false, nil
	}

	return true, nil
}

func (ac AccountCredited) accountCreditedAccountDetailsKeyMapping(_, value interface{}) (interface{}, error) {

	accCredited, _ := value.(events.AccountCredited)

	return accCredited.Body.AccountNo, nil
}

func (ac AccountCredited) accountCreditedAccountDetailsMapping(left interface{}, right interface{}) (joined interface{}, err error) {

	l, _ := left.(events.AccountCredited)
	r, _ := right.(events.AccountDetailsUpdated)

	dateTime := time.Unix(l.Body.CreditedAt, 0).Format(time.RFC1123)
	text := fmt.Sprintf(`Your a/c %d is credited with %v USD on %v at %v`, l.Body.AccountNo, l.Body.Amount, dateTime, l.Body.Location)

	message := events.MessageCreated{
		ID:        uuid.New().String(),
		Type:      "message_created",
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	message.Body.CustomerID = r.Body.CustomerID
	message.Body.Text = text

	return message, nil
}

func (ac AccountCredited) accountCreditedMessageCustomerProfileKeyMapping(key interface{}, value interface{}) (mappedKey interface{}, err error) {

	message, _ := value.(events.MessageCreated)

	return message.Body.CustomerID, nil
}

func (ac AccountCredited) accountMessageCustomerProfileDetailsMapping(left interface{}, right interface{}) (joined interface{}, err error) {

	l, _ := left.(events.MessageCreated)
	r, _ := right.(events.CustomerProfileUpdated)

	l.Body.Address = r.Body.ContactDetails.Address
	l.Body.Phone = r.Body.ContactDetails.Phone
	l.Body.Email = r.Body.ContactDetails.Email

	return l, nil
}
