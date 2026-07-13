package operator

import (
	"context"
	"sync"
)

// fakeTemporal is an in-memory TemporalNamespaceRegisterer for reconciler
// tests — avoids needing a real Temporal frontend just to exercise reconcile
// logic.
type fakeTemporal struct {
	mu         sync.Mutex
	registered map[string]int
	err        error
}

func newFakeTemporal() *fakeTemporal {
	return &fakeTemporal{registered: map[string]int{}}
}

func (f *fakeTemporal) RegisterNamespace(_ context.Context, namespace string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.registered[namespace]++
	return nil
}

func (f *fakeTemporal) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeTemporal) count(namespace string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registered[namespace]
}
