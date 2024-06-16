package redis

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

func TestDistlock(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if len(addr) == 0 {
		t.Log("no env provided, can't test")
		return
	}
	client0 := redis.NewClient(&redis.Options{Addr: addr})
	distlock0 := Distlock{Client: client0}

	client1 := redis.NewClient(&redis.Options{Addr: addr})
	distlock1 := Distlock{Client: client1}

	t.Run("consecutive", func(t *testing.T) {
		key := fmt.Sprintf("lock%d", time.Now().Nanosecond())
		// set the lock in first thread
		if ok, err := distlock0.Lock(key, "foobar"); !ok || err != nil {
			t.Error(err)
		}

		// try lock in second thread, must fail
		if ok, _ := distlock1.Lock(key, "qwerty"); ok {
			t.Error("second lock must fail")
		}

		// unlock in first thread
		if err := distlock0.Unlock(key, "foobar"); err != nil {
			t.Error(err)
		}

		// successfully set the lock in second thread
		if ok, err := distlock1.Lock(key, "qwerty"); !ok || err != nil {
			t.Error(err)
		}
	})
	t.Run("parallel", func(t *testing.T) {
		key := fmt.Sprintf("lock%d", time.Now().Nanosecond())
		var wg sync.WaitGroup
		var c int
		thread := func(ctx context.Context, distlock *Distlock, key, secret string) {
			defer wg.Done()
			var ack bool
			var i int
			for {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(time.Millisecond)
					if !ack {
						if ack, _ = distlock.Lock(key, secret); !ack {
							continue
						}
					}
					c++
					if i += 1; i == 10 {
						ack = false
						i = 0
						_ = distlock.Unlock(key, secret)
					}
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		wg.Add(2)
		go thread(ctx, &distlock0, key, "foobar")
		go thread(ctx, &distlock1, key, "qwerty")
		wg.Wait()
		t.Logf("collected %d", c)
		_ = cancel
	})
}
