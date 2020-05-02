package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./sources.go -destination=./sources_mock_test.go -package=loaders_test

// SourceRepository is the datastore repository for sources
type SourceRepository interface {
	ComponentBySources(ctx context.Context, sources []int) ([]*model.Component, error)
	PodsBySources(ctx context.Context, sources []int) ([][]*model.Pod, error)
	TopicBySources(ctx context.Context, sources []int) ([]*model.Topic, error)
}

var _ resolvers.SourceLoader = &SourceLoader{}

// SourceLoader contains data loaders for source relationships
type SourceLoader struct {
	componentBySource *generated.ComponentLoader
	podsBySource      *generated.PodSliceLoader
	topicBySource     *generated.TopicLoader
}

// NewSourceLoader creates a new SourceLoader
func NewSourceLoader(ctx context.Context, repository SourceRepository, waitTime time.Duration) *SourceLoader {
	loader := &SourceLoader{}

	loader.componentBySource = generated.NewComponentLoader(generated.ComponentLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentBySources(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get component from repository")}
			}

			return r, nil
		},
	})

	loader.podsBySource = generated.NewPodSliceLoader(generated.PodSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsBySources(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get pods from repository")}
			}

			return r, nil
		},
	})

	loader.topicBySource = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicBySources(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ComponentBySource returns the component for the source
func (l *SourceLoader) ComponentBySource(sourceID int) (*model.Component, error) {
	return l.componentBySource.Load(sourceID)
}

// PodsBySource returns the pods for the source
func (l *SourceLoader) PodsBySource(sourceID int) ([]*model.Pod, error) {
	return l.podsBySource.Load(sourceID)
}

// TopicBySource returns the topic for the source
func (l *SourceLoader) TopicBySource(sourceID int) (*model.Topic, error) {
	return l.topicBySource.Load(sourceID)
}
