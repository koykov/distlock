package aerospike

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
)

func TestDistlock(t *testing.T) {
	host := os.Getenv("AEROSPIKE_HOST")
	port, err := strconv.Atoi(os.Getenv("AEROSPIKE_PORT"))
	if err != nil || port <= 0 || port > 65535 || len(host) == 0 {
		t.Log("no env provided, can't test")
		return
	}
	ns, set := os.Getenv("AEROSPIKE_NS"), os.Getenv("AEROSPIKE_SET")

	client0, err := as.NewClient(host, port)
	if err != nil {
		t.Fatal(err)
	}
	policy0 := as.NewWritePolicy(0, uint32(3600))
	policy0.RecordExistsAction = as.CREATE_ONLY
	policy0.CommitLevel = as.COMMIT_MASTER

	distlock0 := Distlock{
		Namespace: ns,
		SetName:   set,
		Bins:      []string{"lock_flag"},
		Policy:    policy0,
		Client:    client0,
	}

	client1, err := as.NewClient(host, port)
	if err != nil {
		t.Fatal(err)
	}
	policy1 := as.NewWritePolicy(0, uint32(3600))
	policy1.RecordExistsAction = as.CREATE_ONLY
	policy1.CommitLevel = as.COMMIT_MASTER

	distlock1 := Distlock{
		Namespace: ns,
		SetName:   set,
		Bins:      []string{"lock_flag"},
		Policy:    policy1,
		Client:    client1,
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
