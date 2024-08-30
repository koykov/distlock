package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync"

	clnv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Distlock struct {
	Session *concurrency.Session

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

	keyL := fmt.Sprintf("%s%x", key, l.Session.Lease())
	cmp := clnv3.Compare(clnv3.CreateRevision(keyL), "=", 0)
	put := clnv3.OpPut(keyL, secret, clnv3.WithLease(l.Session.Lease()))
	get := clnv3.OpGet(keyL)
	getOwner := clnv3.OpGet(key, clnv3.WithFirstCreate()...)
	resp, err := l.Session.Client().Txn(ctx).
		If(cmp).
		Then(put, getOwner).
		Else(get, getOwner).
		Commit()
	if err != nil {
		return false, err
	}
	rev := resp.Header.Revision
	if !resp.Succeeded {
		rev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}

	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == rev {
		// m.hdr = resp.Header
		return true, nil
	}

	return false, ErrLocked
}

func (l *Distlock) Unlock(key, secret string) error {
	return l.UnlockContext(context.Background(), key, secret)
}

func (l *Distlock) UnlockContext(ctx context.Context, key, secret string) error {
	l.once.Do(l.init)
	if l.err != nil {
		return l.err
	}
	keyL := fmt.Sprintf("%s%x", key, l.Session.Lease())
	resp, err := l.Session.Client().Txn(ctx).
		If(clnv3.Compare(clnv3.Value(keyL), "=", secret)).
		Then(clnv3.OpDelete(keyL)).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrLocked
	}
	return nil
}

func (l *Distlock) init() {
	if l.Session == nil {
		l.err = ErrNoSession
	}
}

var (
	ErrNoSession = errors.New("no session provided")
	ErrLocked    = errors.New("locked in another session")
)
