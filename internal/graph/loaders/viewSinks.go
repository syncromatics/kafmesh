package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ViewSinkRepository is the datastore repository for view sinks
type ViewSinkRepository interface {
	ComponentByViewSinks(ctx context.Context, viewSinks []int) ([]*model.Component, error)
	PodsByViewSinks(ctx context.Context, viewSinks []int) ([][]*model.Pod, error)
	TopicByViewSinks(ctx context.Context, viewSinks []int) ([]*model.Topic, error)
}

var _ resolvers.ViewSinkLoader = &ViewSinkLoader{}

// ViewSinkLoader contains data loaders for view sink relationships
type ViewSinkLoader struct {
	componentByViewSink *componentLoader
	podsByViewSink      *podSliceLoader
	topicByViewSink     *topicLoader
}

// NewViewSinkLoader creates a new ViewSinkLoader
func NewViewSinkLoader(ctx context.Context, repository ViewSinkRepository) *ViewSinkLoader {
	loader := &ViewSinkLoader{}

	loader.componentByViewSink = &componentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.podsByViewSink = &podSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.topicByViewSink = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	return loader
}

// ComponentByViewSink returns the component for the view sink
func (l *ViewSinkLoader) ComponentByViewSink(viewSinkID int) (*model.Component, error) {
	return l.componentByViewSink.Load(viewSinkID)
}

// PodsByViewSink returns the pods for the view sink
func (l *ViewSinkLoader) PodsByViewSink(viewSinkID int) ([]*model.Pod, error) {
	return l.podsByViewSink.Load(viewSinkID)
}

// TopicByViewSink returns the topic for the view sink
func (l *ViewSinkLoader) TopicByViewSink(viewSinkID int) (*model.Topic, error) {
	return l.topicByViewSink.Load(viewSinkID)
}
