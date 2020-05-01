package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// SourceRepository is the datastore repository for sources
type SourceRepository interface {
	ComponentBySources(ctx context.Context, sources []int) ([]*model.Component, error)
	PodsBySources(ctx context.Context, sources []int) ([][]*model.Pod, error)
	TopicBySources(ctx context.Context, sources []int) ([]*model.Topic, error)
}

var _ resolvers.SourceLoader = &SourceLoader{}

// SourceLoader contains data loaders for source relationships
type SourceLoader struct {
	componentBySource *componentLoader
	podsBySource      *podSliceLoader
	topicBySource     *topicLoader
}

// NewSourceLoader creates a new SourceLoader
func NewSourceLoader(ctx context.Context, repository SourceRepository) *SourceLoader {
	loader := &SourceLoader{}

	loader.componentBySource = &componentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentBySources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.podsBySource = &podSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsBySources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.topicBySource = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicBySources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

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
