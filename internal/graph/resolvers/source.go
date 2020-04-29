package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.SourceResolver = &SourceResolver{}

// SourceResolver resolves the source's relationships
type SourceResolver struct {
	loaders loaderFunc
}

// Component returns the source's component
func (r *SourceResolver) Component(ctx context.Context, source *model.Source) (*model.Component, error) {
	return r.loaders(ctx).SourceLoader.ComponentBySource.Load(source.ID)
}

// Pods returns the source's pods
func (r *SourceResolver) Pods(ctx context.Context, source *model.Source) ([]*model.Pod, error) {
	return r.loaders(ctx).SourceLoader.PodsBySource.Load(source.ID)
}

// Topic returns the source's topic
func (r *SourceResolver) Topic(ctx context.Context, source *model.Source) (*model.Topic, error) {
	return r.loaders(ctx).SourceLoader.TopicBySource.Load(source.ID)
}
