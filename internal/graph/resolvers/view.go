package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ViewResolver = &ViewResolver{}

// ViewResolver resolves the view's relationships
type ViewResolver struct {
	loaders loaderFunc
}

// Component returns the view's component
func (r *ViewResolver) Component(ctx context.Context, view *model.View) (*model.Component, error) {
	return r.loaders(ctx).ViewLoader.ComponentByView.Load(view.ID)
}

// Pods returns the view's pods
func (r *ViewResolver) Pods(ctx context.Context, view *model.View) ([]*model.Pod, error) {
	return r.loaders(ctx).ViewLoader.PodsByView.Load(view.ID)
}

// Topic returns the view's topic
func (r *ViewResolver) Topic(ctx context.Context, view *model.View) (*model.Topic, error) {
	return r.loaders(ctx).ViewLoader.TopicByView.Load(view.ID)
}
