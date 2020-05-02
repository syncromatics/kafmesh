package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./views.go -destination=./views_mock_test.go -package=loaders_test

// ViewRepository is the datastore repository for views
type ViewRepository interface {
	ComponentByViews(ctx context.Context, views []int) ([]*model.Component, error)
	PodsByViews(ctx context.Context, views []int) ([][]*model.Pod, error)
	TopicByViews(ctx context.Context, views []int) ([]*model.Topic, error)
}

var _ resolvers.ViewLoader = &ViewLoader{}

// ViewLoader contains data loaders for view relationships
type ViewLoader struct {
	componentByView *generated.ComponentLoader
	podsByView      *generated.PodSliceLoader
	topicByView     *generated.TopicLoader
}

// NewViewLoader creates a new ViewLoader
func NewViewLoader(ctx context.Context, repository ViewRepository, waitTime time.Duration) *ViewLoader {
	loader := &ViewLoader{}

	loader.componentByView = generated.NewComponentLoader(generated.ComponentLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByViews(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get component from repository")}
			}

			return r, nil
		},
	})

	loader.podsByView = generated.NewPodSliceLoader(generated.PodSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByViews(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get pods from repository")}
			}

			return r, nil
		},
	})

	loader.topicByView = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByViews(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ComponentByView returns the component for the view
func (l *ViewLoader) ComponentByView(viewID int) (*model.Component, error) {
	return l.componentByView.Load(viewID)
}

// PodsByView returns the pods for the view
func (l *ViewLoader) PodsByView(viewID int) ([]*model.Pod, error) {
	return l.podsByView.Load(viewID)
}

// TopicByView returns the topic for the view
func (l *ViewLoader) TopicByView(viewID int) (*model.Topic, error) {
	return l.topicByView.Load(viewID)
}
