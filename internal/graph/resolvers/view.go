package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./view.go -destination=./view_mock_test.go -package=resolvers_test

// ViewLoader is the dataloader for a view
type ViewLoader interface {
	ComponentByView(int) (*model.Component, error)
	PodsByView(int) ([]*model.Pod, error)
	TopicByView(int) (*model.Topic, error)
}

var _ generated.ViewResolver = &ViewResolver{}

// ViewResolver resolves the view's relationships
type ViewResolver struct {
	*Resolver
}

// Component returns the view's component
func (r *ViewResolver) Component(ctx context.Context, view *model.View) (*model.Component, error) {
	results, err := r.DataLoaders.ViewLoader(ctx).ComponentByView(view.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get component from loader")
	}
	return results, nil
}

// Pods returns the view's pods
func (r *ViewResolver) Pods(ctx context.Context, view *model.View) ([]*model.Pod, error) {
	results, err := r.DataLoaders.ViewLoader(ctx).PodsByView(view.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return results, nil
}

// Topic returns the view's topic
func (r *ViewResolver) Topic(ctx context.Context, view *model.View) (*model.Topic, error) {
	results, err := r.DataLoaders.ViewLoader(ctx).TopicByView(view.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return results, nil
}
