package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./viewSinks.go -destination=./viewSinks_mock_test.go -package=loaders_test

// ViewSinkRepository is the datastore repository for view sinks
type ViewSinkRepository interface {
	ComponentByViewSinks(ctx context.Context, viewSinks []int) ([]*model.Component, error)
	PodsByViewSinks(ctx context.Context, viewSinks []int) ([][]*model.Pod, error)
	TopicByViewSinks(ctx context.Context, viewSinks []int) ([]*model.Topic, error)
}

var _ resolvers.ViewSinkLoader = &ViewSinkLoader{}

// ViewSinkLoader contains data loaders for view sink relationships
type ViewSinkLoader struct {
	componentByViewSink *generated.ComponentLoader
	podsByViewSink      *generated.PodSliceLoader
	topicByViewSink     *generated.TopicLoader
}

// NewViewSinkLoader creates a new ViewSinkLoader
func NewViewSinkLoader(ctx context.Context, repository ViewSinkRepository, waitTime time.Duration) *ViewSinkLoader {
	loader := &ViewSinkLoader{}

	loader.componentByViewSink = generated.NewComponentLoader(generated.ComponentLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get component from repository")}
			}

			return r, nil
		},
	})

	loader.podsByViewSink = generated.NewPodSliceLoader(generated.PodSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get pods from repository")}
			}

			return r, nil
		},
	})

	loader.topicByViewSink = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByViewSinks(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}

			return r, nil
		},
	})

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
