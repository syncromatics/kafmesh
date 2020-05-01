package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ViewSourceRepository is the datastore repository for view sources
type ViewSourceRepository interface {
	ComponentByViewSources(ctx context.Context, viewSources []int) ([]*model.Component, error)
	PodsByViewSources(ctx context.Context, viewSources []int) ([][]*model.Pod, error)
	TopicByViewSources(ctx context.Context, viewSources []int) ([]*model.Topic, error)
}

var _ resolvers.ViewSourceLoader = &ViewSourceLoader{}

// ViewSourceLoader contains data loaders for view source relationships
type ViewSourceLoader struct {
	componentByViewSource *componentLoader
	podsByViewSource      *podSliceLoader
	topicByViewSource     *topicLoader
}

// NewViewSourceLoader creates a new ViewSourceLoader
func NewViewSourceLoader(ctx context.Context, repository ViewSourceRepository) *ViewSourceLoader {
	loader := &ViewSourceLoader{}

	loader.componentByViewSource = &componentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByViewSources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.podsByViewSource = &podSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByViewSources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.topicByViewSource = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByViewSources(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	return loader
}

// ComponentByViewSource returns the component for the view source
func (l *ViewSourceLoader) ComponentByViewSource(viewSourceID int) (*model.Component, error) {
	return l.componentByViewSource.Load(viewSourceID)
}

// PodsByViewSource returns the pods for the view source
func (l *ViewSourceLoader) PodsByViewSource(viewSourceID int) ([]*model.Pod, error) {
	return l.podsByViewSource.Load(viewSourceID)
}

// TopicByViewSource returns the topic for the view source
func (l *ViewSourceLoader) TopicByViewSource(viewSourceID int) (*model.Topic, error) {
	return l.topicByViewSource.Load(viewSourceID)
}
