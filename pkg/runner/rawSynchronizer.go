package runner

import (
	"context"

	"github.com/lovoo/goka"
	"github.com/pkg/errors"
)

// RawViewSourceJob executes a raw bytes synchronize
type RawViewSourceJob struct {
	context.Context
	view     *goka.View
	emitter  *Emitter
	keysSeen map[string]struct{}
}

// NewRawViewSourceJob creates a new raw bytes view source job
func NewRawViewSourceJob(ctx context.Context, view *goka.View, emitter *Emitter) *RawViewSourceJob {
	keysSeen := map[string]struct{}{}
	return &RawViewSourceJob{
		ctx,
		view,
		emitter,
		keysSeen,
	}
}

// Update adds a key/value pair to the job
func (s *RawViewSourceJob) Update(key string, msg []byte) error {
	s.keysSeen[key] = struct{}{}

	current, err := s.view.Get(key)
	if err != nil {
		return errors.Wrap(err, "failed to get object")
	}

	var shouldUpdate = false

	if current == nil {
		shouldUpdate = true
	} else {
		c := current.([]byte)
		if len(c) != len(msg) {
			shouldUpdate = true
		} else {
			for i, b := range c {
				if msg[i] != b {
					shouldUpdate = true
					break
				}
			}
		}
	}

	if !shouldUpdate {
		return nil
	}

	err = s.emitter.Emit(key, msg)
	if err != nil {
		return errors.Wrap(err, "failed to emit update")
	}

	return nil
}

// Finish the job and run deletes
func (s *RawViewSourceJob) Finish() error {
	currentKeys, err := s.keys()
	if err != nil {
		return errors.Wrap(err, "failed to get current keys")
	}

	for _, k := range currentKeys {
		_, ok := s.keysSeen[k]
		if ok {
			continue
		}

		err = s.emitter.Delete(k)
		if err != nil {
			return errors.Wrap(err, "failed to delete key")
		}
	}

	return nil
}

func (s *RawViewSourceJob) keys() ([]string, error) {
	it, err := s.view.Iterator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get iterator")
	}

	keys := []string{}
	for it.Next() {
		keys = append(keys, it.Key())
	}

	return keys, nil
}
