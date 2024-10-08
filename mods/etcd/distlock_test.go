package etcd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestDistlock(t *testing.T) {
	host := os.Getenv("ETCD_HOST")
	port, err := strconv.Atoi(os.Getenv("ETCD_PORT"))
	if err != nil || port <= 0 || port > 65535 || len(host) == 0 {
		t.Log("no env provided, can't test")
		return
	}

	distlock0 := Distlock{
		// todo init session #1
	}

	distlock1 := Distlock{
		// todo init session #2
	}

	t.Run("consecutive", func(t *testing.T) {
		key := fmt.Sprintf("lock%d", time.Now().Nanosecond())
		// set the lock in first thread
		if ok, err := distlock0.Lock(key, "foobar"); !ok || err != nil {
			t.Error(err)
		}

		// try lock in second thread, must fail
		if ok, err := distlock1.Lock(key, "qwerty"); ok || err == nil {
			t.Error("second lock must fail")
		}

		// unlock in first thread
		if err = distlock0.Unlock(key, "foobar"); err != nil {
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
