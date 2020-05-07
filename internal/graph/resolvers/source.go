package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./source.go -destination=./source_mock_test.go -package=resolvers_test

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
	results, err := r.DataLoaders.SourceLoader(ctx).ComponentBySource(source.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get component from loader")
	}
	return results, nil
}

// Pods returns the source's pods
func (r *SourceResolver) Pods(ctx context.Context, source *model.Source) ([]*model.Pod, error) {
	results, err := r.DataLoaders.SourceLoader(ctx).PodsBySource(source.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return results, nil
}

// Topic returns the source's topic
func (r *SourceResolver) Topic(ctx context.Context, source *model.Source) (*model.Topic, error) {
	results, err := r.DataLoaders.SourceLoader(ctx).TopicBySource(source.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return results, nil
}
