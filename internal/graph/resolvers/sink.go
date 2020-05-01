package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// SinkLoader is the dataloader for a sink
type SinkLoader interface {
	ComponentBySink(int) (*model.Component, error)
	PodsBySink(int) ([]*model.Pod, error)
	TopicBySink(int) (*model.Topic, error)
}

var _ generated.SinkResolver = &SinkResolver{}

// SinkResolver resolves the sink's relationships
type SinkResolver struct {
	*Resolver
}

// Component returns the sink's component
func (r *SinkResolver) Component(ctx context.Context, sink *model.Sink) (*model.Component, error) {
	return r.DataLoaders.SinkLoader(ctx).ComponentBySink(sink.ID)
}

// Pods returns the sink's pods
func (r *SinkResolver) Pods(ctx context.Context, sink *model.Sink) ([]*model.Pod, error) {
	return r.DataLoaders.SinkLoader(ctx).PodsBySink(sink.ID)
}

// Topic returns the sink's topic
func (r *SinkResolver) Topic(ctx context.Context, sink *model.Sink) (*model.Topic, error) {
	return r.DataLoaders.SinkLoader(ctx).TopicBySink(sink.ID)
}
