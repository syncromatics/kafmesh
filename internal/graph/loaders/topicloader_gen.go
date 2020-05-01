// Code generated by github.com/vektah/dataloaden, DO NOT EDIT.

package loaders

import (
	"sync"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// topicLoaderConfig captures the config to create a new topicLoader
type topicLoaderConfig struct {
	// Fetch is a method that provides the data for the loader
	Fetch func(keys []int) ([]*model.Topic, []error)

	// Wait is how long wait before sending a batch
	Wait time.Duration

	// MaxBatch will limit the maximum number of keys to send in one batch, 0 = not limit
	MaxBatch int
}

// NewtopicLoader creates a new topicLoader given a fetch, wait, and maxBatch
func NewtopicLoader(config topicLoaderConfig) *topicLoader {
	return &topicLoader{
		fetch:    config.Fetch,
		wait:     config.Wait,
		maxBatch: config.MaxBatch,
	}
}

// topicLoader batches and caches requests
type topicLoader struct {
	// this method provides the data for the loader
	fetch func(keys []int) ([]*model.Topic, []error)

	// how long to done before sending a batch
	wait time.Duration

	// this will limit the maximum number of keys to send in one batch, 0 = no limit
	maxBatch int

	// INTERNAL

	// lazily created cache
	cache map[int]*model.Topic

	// the current batch. keys will continue to be collected until timeout is hit,
	// then everything will be sent to the fetch method and out to the listeners
	batch *topicLoaderBatch

	// mutex to prevent races
	mu sync.Mutex
}

type topicLoaderBatch struct {
	keys    []int
	data    []*model.Topic
	error   []error
	closing bool
	done    chan struct{}
}

// Load a Topic by key, batching and caching will be applied automatically
func (l *topicLoader) Load(key int) (*model.Topic, error) {
	return l.LoadThunk(key)()
}

// LoadThunk returns a function that when called will block waiting for a Topic.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *topicLoader) LoadThunk(key int) func() (*model.Topic, error) {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() (*model.Topic, error) {
			return it, nil
		}
	}
	if l.batch == nil {
		l.batch = &topicLoaderBatch{done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()

	return func() (*model.Topic, error) {
		<-batch.done

		var data *model.Topic
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		var err error
		// its convenient to be able to return a single error for everything
		if len(batch.error) == 1 {
			err = batch.error[0]
		} else if batch.error != nil {
			err = batch.error[pos]
		}

		if err == nil {
			l.mu.Lock()
			l.unsafeSet(key, data)
			l.mu.Unlock()
		}

		return data, err
	}
}

// LoadAll fetches many keys at once. It will be broken into appropriate sized
// sub batches depending on how the loader is configured
func (l *topicLoader) LoadAll(keys []int) ([]*model.Topic, []error) {
	results := make([]func() (*model.Topic, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	topics := make([]*model.Topic, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		topics[i], errors[i] = thunk()
	}
	return topics, errors
}

// LoadAllThunk returns a function that when called will block waiting for a Topics.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *topicLoader) LoadAllThunk(keys []int) func() ([]*model.Topic, []error) {
	results := make([]func() (*model.Topic, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]*model.Topic, []error) {
		topics := make([]*model.Topic, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			topics[i], errors[i] = thunk()
		}
		return topics, errors
	}
}

// Prime the cache with the provided key and value. If the key already exists, no change is made
// and false is returned.
// (To forcefully prime the cache, clear the key first with loader.clear(key).prime(key, value).)
func (l *topicLoader) Prime(key int, value *model.Topic) bool {
	l.mu.Lock()
	var found bool
	if _, found = l.cache[key]; !found {
		// make a copy when writing to the cache, its easy to pass a pointer in from a loop var
		// and end up with the whole cache pointing to the same value.
		cpy := *value
		l.unsafeSet(key, &cpy)
	}
	l.mu.Unlock()
	return !found
}

// Clear the value at key from the cache, if it exists
func (l *topicLoader) Clear(key int) {
	l.mu.Lock()
	delete(l.cache, key)
	l.mu.Unlock()
}

func (l *topicLoader) unsafeSet(key int, value *model.Topic) {
	if l.cache == nil {
		l.cache = map[int]*model.Topic{}
	}
	l.cache[key] = value
}

// keyIndex will return the location of the key in the batch, if its not found
// it will add the key to the batch
func (b *topicLoaderBatch) keyIndex(l *topicLoader, key int) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}

	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}

	if l.maxBatch != 0 && pos >= l.maxBatch-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}

	return pos
}

func (b *topicLoaderBatch) startTimer(l *topicLoader) {
	time.Sleep(l.wait)
	l.mu.Lock()

	// we must have hit a batch limit and are already finalizing this batch
	if b.closing {
		l.mu.Unlock()
		return
	}

	l.batch = nil
	l.mu.Unlock()

	b.end(l)
}

func (b *topicLoaderBatch) end(l *topicLoader) {
	b.data, b.error = l.fetch(b.keys)
	close(b.done)
}
