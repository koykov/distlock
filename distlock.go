package distlock

import (
	"context"
)

type Interface interface {
	Lock(key, secret string) (bool, error)
	LockContext(ctx context.Context, key, secret string) (bool, error)

	Unlock(key, secret string) error
	UnlockContext(ctx context.Context, key, secret string) error
}
