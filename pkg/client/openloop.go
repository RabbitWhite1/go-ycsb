// Copyright 2026 go-ycsb authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

// Open-loop mode (openloop=true): each worker issues its transactions at fixed intended times
// (start + i / per-thread target), never waiting for earlier operations. The workload still generates
// every operation on the worker goroutine (so its per-thread state is not shared); only the DB call runs
// asynchronously, in DbWrapper, and its latency is measured from the intended time. This is the
// semantics of YCSB's async bindings (zookeeper.async / redis.async / etcd.async in RabbitWhite1/YCSB)
// with measurement.interval=intended.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type intendedStartKey struct{}

var (
	inflight      sync.WaitGroup
	inflightCount int64
)

func withIntendedStart(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, intendedStartKey{}, t)
}

// intendedStart returns the operation's intended start when it was issued by an open-loop worker.
func intendedStart(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(intendedStartKey{}).(time.Time)
	return t, ok
}

// goAsync runs one operation in the background, counted for the final drain.
func goAsync(f func()) {
	inflight.Add(1)
	atomic.AddInt64(&inflightCount, 1)
	go func() {
		defer func() {
			atomic.AddInt64(&inflightCount, -1)
			inflight.Done()
		}()
		f()
	}()
}

// drainInflight waits for every background operation, up to timeout; returns how many remain.
func drainInflight(timeout time.Duration) int64 {
	done := make(chan struct{})
	go func() {
		inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return 0
	case <-time.After(timeout):
		return atomic.LoadInt64(&inflightCount)
	}
}

// copyValues deep-copies a record: the workload returns value buffers to its pool once the
// (asynchronous) call returns.
func copyValues(values map[string][]byte) map[string][]byte {
	c := make(map[string][]byte, len(values))
	for k, v := range values {
		c[k] = append([]byte(nil), v...)
	}
	return c
}
