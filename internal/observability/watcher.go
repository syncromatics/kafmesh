package observability

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"github.com/pkg/errors"
)

type processorWatchKey struct {
	component string
	processor string
	key       string
}

type watch struct {
	send   func(*watchv1.ProcessorResponse) error
	cancel func()
}

// Watcher sends observers information about the running system
type Watcher struct {
	mtx     sync.RWMutex
	watches map[processorWatchKey]map[string]*watch
}

// WatchProcessor registers an observer for the processor and key
func (w *Watcher) WatchProcessor(ctx context.Context, request *watchv1.ProcessorRequest, send func(*watchv1.ProcessorResponse) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	key := processorWatchKey{
		component: request.Component,
		processor: request.Processor,
		key:       request.Key,
	}

	id, err := makeKey()
	if err != nil {
		return err
	}

	w.registerWatcher(key, id, &watch{
		send:   send,
		cancel: cancel,
	})

	<-ctx.Done()

	w.removeWatcher(key, id)

	return nil
}

// WatchCount returns the number of observers for the processor/key combo
func (w *Watcher) WatchCount(component, processor, key string) (int, bool) {
	w.mtx.RLock()
	defer w.mtx.RUnlock()

	m, ok := w.watches[processorWatchKey{component, processor, key}]
	if !ok {
		return 0, false
	}
	return len(m), true
}

// Send notifies observers about an event of a processor/key
func (w *Watcher) Send(component, processor, key string, message *watchv1.Operation) {
	w.mtx.RLock()
	defer w.mtx.RUnlock()

	m, ok := w.watches[processorWatchKey{
		component: component,
		processor: processor,
		key:       key,
	}]
	if !ok {
		return
	}

	for _, s := range m {
		err := s.send(&watchv1.ProcessorResponse{
			Operation: message,
		})
		if err != nil {
			s.cancel()
		}
	}

	return
}

func (w *Watcher) registerWatcher(key processorWatchKey, id string, processorWatch *watch) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.watches == nil {
		w.watches = map[processorWatchKey]map[string]*watch{}
	}

	_, ok := w.watches[key]
	if !ok {
		w.watches[key] = map[string]*watch{}
	}

	m := w.watches[key]

	_, ok = m[id]
	if ok {
		panic("key exists")
	}

	m[id] = processorWatch
}

func (w *Watcher) removeWatcher(key processorWatchKey, id string) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	m, ok := w.watches[key]
	if !ok {
		return
	}
	_, ok = m[id]
	if !ok {
		return
	}

	delete(m, id)

	if len(m) == 0 {
		delete(w.watches, key)
	}
}

func makeKey() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", errors.Wrap(err, "failed to create key")
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
