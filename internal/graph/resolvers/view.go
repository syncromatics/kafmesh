package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.ViewLoader(ctx).ComponentByView(view.ID)
}

// Pods returns the view's pods
func (r *ViewResolver) Pods(ctx context.Context, view *model.View) ([]*model.Pod, error) {
	return r.DataLoaders.ViewLoader(ctx).PodsByView(view.ID)
}

// Topic returns the view's topic
func (r *ViewResolver) Topic(ctx context.Context, view *model.View) (*model.Topic, error) {
	return r.DataLoaders.ViewLoader(ctx).TopicByView(view.ID)
}
