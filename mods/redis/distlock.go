package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type Distlock struct {
	Client *redis.Client
	TTL    time.Duration

	once sync.Once
	err  error
}

func (l *Distlock) Lock(key, secret string) (bool, error) {
	l.once.Do(l.init)
	if l.err != nil {
		return false, l.err
	}
	return l.LockContext(context.Background(), key, secret)
}

func (l *Distlock) LockContext(ctx context.Context, key, secret string) (ok bool, err error) {
	l.once.Do(l.init)
	if l.err != nil {
		return false, l.err
	}

	done := make(chan struct{})
	go func() {
		ok, err = l.Client.SetNX(key, secret, l.TTL).Result()
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-done:
		return
	}
}

func (l *Distlock) Unlock(key, secret string) error {
	return l.UnlockContext(context.Background(), key, secret)
}

func (l *Distlock) UnlockContext(ctx context.Context, key, secret string) (err error) {
	l.once.Do(l.init)
	if l.err != nil {
		return l.err
	}

	scr := redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end`)

	done := make(chan struct{})
	go func() {
		keys := []string{key}
		secrets := []string{secret}
		cmd := scr.Run(l.Client, keys, secrets)
		err = cmd.Err()
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return
	}
}

func (l *Distlock) init() {
	if l.Client == nil {
		l.err = ErrNoClient
		return
	}
}

var ErrNoClient = errors.New("no client provided")
