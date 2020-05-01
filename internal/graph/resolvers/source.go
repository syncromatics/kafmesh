package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// SourceLoader is the dataloader for sources
type SourceLoader interface {
	ComponentBySource(int) (*model.Component, error)
	PodsBySource(int) ([]*model.Pod, error)
	TopicBySource(int) (*model.Topic, error)
}

var _ generated.SourceResolver = &SourceResolver{}

// SourceResolver resolves the source's relationships
type SourceResolver struct {
	*Resolver
}

// Component returns the source's component
func (r *SourceResolver) Component(ctx context.Context, source *model.Source) (*model.Component, error) {
	return r.DataLoaders.SourceLoader(ctx).ComponentBySource(source.ID)
}

// Pods returns the source's pods
func (r *SourceResolver) Pods(ctx context.Context, source *model.Source) ([]*model.Pod, error) {
	return r.DataLoaders.SourceLoader(ctx).PodsBySource(source.ID)
}

// Topic returns the source's topic
func (r *SourceResolver) Topic(ctx context.Context, source *model.Source) (*model.Topic, error) {
	return r.DataLoaders.SourceLoader(ctx).TopicBySource(source.ID)
}
