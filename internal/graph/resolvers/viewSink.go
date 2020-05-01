package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ViewSinkLoader is the dataloader for a view sink
type ViewSinkLoader interface {
	ComponentByViewSink(int) (*model.Component, error)
	PodsByViewSink(int) ([]*model.Pod, error)
	TopicByViewSink(int) (*model.Topic, error)
}

var _ generated.ViewSinkResolver = &ViewSinkResolver{}

// ViewSinkResolver resolves the view sink's relationships
type ViewSinkResolver struct {
	*Resolver
}

// Component returns the view sink's component
func (r *ViewSinkResolver) Component(ctx context.Context, viewSink *model.ViewSink) (*model.Component, error) {
	return r.DataLoaders.ViewSinkLoader(ctx).ComponentByViewSink(viewSink.ID)
}

// Pods returns the view sink's pods
func (r *ViewSinkResolver) Pods(ctx context.Context, viewSink *model.ViewSink) ([]*model.Pod, error) {
	return r.DataLoaders.ViewSinkLoader(ctx).PodsByViewSink(viewSink.ID)
}

// Topic returns the view sink's topic
func (r *ViewSinkResolver) Topic(ctx context.Context, viewSink *model.ViewSink) (*model.Topic, error) {
	return r.DataLoaders.ViewSinkLoader(ctx).TopicByViewSink(viewSink.ID)
}
