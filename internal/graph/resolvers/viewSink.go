package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ViewSinkResolver = &ViewSinkResolver{}

// ViewSinkResolver resolves the view sink's relationships
type ViewSinkResolver struct {
	loaders loaderFunc
}

// Component returns the view sink's component
func (r *ViewSinkResolver) Component(ctx context.Context, viewSink *model.ViewSink) (*model.Component, error) {
	return r.loaders(ctx).ViewSinkLoader.ComponentByViewSink.Load(viewSink.ID)
}

// Pods returns the view sink's pods
func (r *ViewSinkResolver) Pods(ctx context.Context, viewSink *model.ViewSink) ([]*model.Pod, error) {
	return r.loaders(ctx).ViewSinkLoader.PodsByViewSink.Load(viewSink.ID)
}

// Topic returns the view sink's topic
func (r *ViewSinkResolver) Topic(ctx context.Context, viewSink *model.ViewSink) (*model.Topic, error) {
	return r.loaders(ctx).ViewSinkLoader.TopicByViewSink.Load(viewSink.ID)
}
