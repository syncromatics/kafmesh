package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ViewSourceResolver = &ViewSourceResolver{}

// ViewSourceResolver resolves the view source's relationships
type ViewSourceResolver struct {
	loaders loaderFunc
}

// Component returns the view source's component
func (r *ViewSourceResolver) Component(ctx context.Context, viewSource *model.ViewSource) (*model.Component, error) {
	return r.loaders(ctx).ViewSourceLoader.ComponentByViewSource.Load(viewSource.ID)
}

// Pods returns the view source's pods
func (r *ViewSourceResolver) Pods(ctx context.Context, viewSource *model.ViewSource) ([]*model.Pod, error) {
	return r.loaders(ctx).ViewSourceLoader.PodsByViewSource.Load(viewSource.ID)
}

// Topic returns the view source's topic
func (r *ViewSourceResolver) Topic(ctx context.Context, viewSource *model.ViewSource) (*model.Topic, error) {
	return r.loaders(ctx).ViewSourceLoader.TopicByViewSource.Load(viewSource.ID)
}
