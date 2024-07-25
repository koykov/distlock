package etcd

import (
	"context"
	"errors"
	"sync"

	cliv3 "go.etcd.io/etcd/client/v3"
)

type Distlock struct {
	Client *cliv3.Client

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
	_, err := l.Client.Put(ctx, key, secret)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (l *Distlock) Unlock(key, secret string) error {
	return l.UnlockContext(context.Background(), key, secret)
}

func (l *Distlock) UnlockContext(ctx context.Context, key, secret string) error {
	l.once.Do(l.init)
	if l.err != nil {
		return l.err
	}
	if _, err := l.Client.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (l *Distlock) init() {
	if l.Client == nil {
		l.err = ErrNoClient
	}
}

var ErrNoClient = errors.New("no client provided")
