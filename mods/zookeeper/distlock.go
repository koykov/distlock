package zookeeper

import (
	"context"
	"sync"

	"github.com/go-zookeeper/zk"
	"github.com/koykov/byteconv"
)

type Distlock struct {
	Conn *zk.Conn
	Path string
	ACL  []zk.ACL

	once sync.Once
	lock *zk.Lock
	err  error
}

func (l *Distlock) Lock(key, secret string) (bool, error) {
	return l.LockContext(context.Background(), key, secret)
}

func (l *Distlock) LockContext(ctx context.Context, key, _ string) (bool, error) {
	l.once.Do(l.init)
	if l.err != nil {
		return false, l.err
	}

	var err error
	done := make(chan struct{})
	go func() {
		err = l.lock.LockWithData(byteconv.S2B(key))
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

func (l *Distlock) UnlockContext(ctx context.Context, key, _ string) error {
	l.once.Do(l.init)
	if l.err != nil {
		return l.err
	}

	var err error
	done := make(chan struct{})
	go func() {
		err = l.lock.Unlock()
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return err
	}
}

func (l *Distlock) init() {
	l.lock = zk.NewLock(l.Conn, l.Path, l.ACL)
}
