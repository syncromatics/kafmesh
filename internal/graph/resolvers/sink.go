package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.SinkResolver = &SinkResolver{}

// SinkResolver resolves the sink's relationships
type SinkResolver struct {
	loaders loaderFunc
}

// Component returns the sink's component
func (r *SinkResolver) Component(ctx context.Context, sink *model.Sink) (*model.Component, error) {
	return r.loaders(ctx).SinkLoader.ComponentBySink.Load(sink.ID)
}

// Pods returns the sink's pods
func (r *SinkResolver) Pods(ctx context.Context, sink *model.Sink) ([]*model.Pod, error) {
	return r.loaders(ctx).SinkLoader.PodsBySink.Load(sink.ID)
}

// Topic returns the sink's topic
func (r *SinkResolver) Topic(ctx context.Context, sink *model.Sink) (*model.Topic, error) {
	return r.loaders(ctx).SinkLoader.TopicBySink.Load(sink.ID)
}
