package aerospike

import (
	"context"
	"sync"

	as "github.com/aerospike/aerospike-client-go"
)

// Distlock implement Aerospike distribution lock.
type Distlock struct {
	Namespace string
	SetName   string
	Bins      []string
	Policy    *as.WritePolicy
	Client    *as.Client

	once sync.Once
	err  error
}

func (l *Distlock) Lock(key, secret string) (bool, error) {
	return l.LockContext(context.Background(), key, secret)
}

func (l *Distlock) LockContext(ctx context.Context, key, secret string) (bool, error) {
	l.once.Do(l.init)
	if l.err != nil {
		return false, l.err
	}
	asKey, err := as.NewKey(l.Namespace, l.SetName, key)
	if err != nil {
		return false, err
	}
	bins := make([]*as.Bin, 0, len(l.Bins))
	for i := 0; i < len(l.Bins); i++ {
		bins = append(bins, as.NewBin(l.Bins[i], secret))
	}

	done := make(chan struct{})
	go func() {
		err = l.Client.PutBins(l.Policy, asKey, bins...)
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-done:
		return err == nil, err
	}
}

func (l *Distlock) Unlock(key, secret string) error {
	return l.UnlockContext(context.Background(), key, secret)
}

func (l *Distlock) UnlockContext(ctx context.Context, key, secret string) error {
	l.once.Do(l.init)
	if l.err != nil {
		return l.err
	}

	asKey, err := as.NewKey(l.Namespace, l.SetName, key)
	if err != nil {
		return err
	}
	bins := make([]*as.Bin, 0, len(l.Bins))
	for i := 0; i < len(l.Bins); i++ {
		bins = append(bins, as.NewBin(l.Bins[i], secret))
	}

	done := make(chan struct{})
	var exists bool
	go func() {
		exists, err = l.Client.Delete(l.Policy, asKey)
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		if !exists {
			return nil
		}
		return err
	}
}

func (l *Distlock) init() {
	if len(l.Namespace) == 0 {
		l.err = ErrNoNS
		return
	}
	if len(l.SetName) == 0 {
		l.err = ErrNoSet
		return
	}
	if len(l.Bins) == 0 {
		l.err = ErrNoBins
		return
	}
	if l.Policy == nil {
		l.err = ErrNoPolicy
		return
	}
	if l.Policy.RecordExistsAction != as.CREATE_ONLY {
		l.err = ErrPolicyFlag
		return
	}
	if l.Client == nil {
		l.err = ErrNoClient
		return
	}
}
