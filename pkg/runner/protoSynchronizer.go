package runner

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"
)

// ProtoSynchronizerJob executes a protobuf synchronize
type ProtoSynchronizerJob struct {
	context.Context
	view     *goka.View
	emitter  *Emitter
	keysSeen map[string]struct{}
}

// NewProtoSynchronizerJob creates a new proto synchronizer job
func NewProtoSynchronizerJob(ctx context.Context, view *goka.View, emitter *Emitter) *ProtoSynchronizerJob {
	keysSeen := map[string]struct{}{}
	return &ProtoSynchronizerJob{
		ctx,
		view,
		emitter,
		keysSeen,
	}
}

// Update adds a key/value pair to the job
func (s *ProtoSynchronizerJob) Update(key string, msg proto.Message) error {
	s.keysSeen[key] = struct{}{}

	current, err := s.view.Get(key)
	if err != nil {
		return errors.Wrap(err, "failed to get object")
	}

	var shouldUpdate = true

	if current != nil {
		c := current.(proto.Message)
		shouldUpdate = c == nil || !proto.Equal(c, msg)
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
func (s *ProtoSynchronizerJob) Finish() error {
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

func (s *ProtoSynchronizerJob) keys() ([]string, error) {
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
