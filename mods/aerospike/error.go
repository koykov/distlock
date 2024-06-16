package aerospike

import "errors"

var (
	ErrNoNS       = errors.New("no namespace provided")
	ErrNoSet      = errors.New("no set name provided")
	ErrNoBins     = errors.New("no bins list provided")
	ErrNoPolicy   = errors.New("no write policy provided")
	ErrPolicyFlag = errors.New("policy.RecordExistsAction must be CREATE_ONLY")
	ErrNoClient   = errors.New("no client provided")
)
