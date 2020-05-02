package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./sinks.go -destination=./sinks_mock_test.go -package=loaders_test

// SinkRepository is the datastore repository for sinks
type SinkRepository interface {
	ComponentBySinks(ctx context.Context, sinks []int) ([]*model.Component, error)
	PodsBySinks(ctx context.Context, sinks []int) ([][]*model.Pod, error)
	TopicBySinks(ctx context.Context, sinks []int) ([]*model.Topic, error)
}

var _ resolvers.SinkLoader = &SinkLoader{}

// SinkLoader contains data loaders for sink relationships
type SinkLoader struct {
	componentBySink *generated.ComponentLoader
	podsBySink      *generated.PodSliceLoader
	topicBySink     *generated.TopicLoader
}

// NewSinkLoader creates a new SinkLoader
func NewSinkLoader(ctx context.Context, repository SinkRepository, waitTime time.Duration) *SinkLoader {
	loader := &SinkLoader{}

	loader.componentBySink = generated.NewComponentLoader(generated.ComponentLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentBySinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get component from repository")}
			}

			return r, nil
		},
	})

	loader.podsBySink = generated.NewPodSliceLoader(generated.PodSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsBySinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get pods from repository")}
			}

			return r, nil
		},
	})

	loader.topicBySink = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicBySinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ComponentBySink returns the component for the sink
func (l *SinkLoader) ComponentBySink(sinkID int) (*model.Component, error) {
	return l.componentBySink.Load(sinkID)
}

// PodsBySink returns the pods for the sink
func (l *SinkLoader) PodsBySink(sinkID int) ([]*model.Pod, error) {
	return l.podsBySink.Load(sinkID)
}

// TopicBySink returns the topic for the sink
func (l *SinkLoader) TopicBySink(sinkID int) (*model.Topic, error) {
	return l.topicBySink.Load(sinkID)
}
