package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./viewSource.go -destination=./viewSource_mock_test.go -package=resolvers_test

// ViewSourceLoader is the data loader for a view source
type ViewSourceLoader interface {
	ComponentByViewSource(int) (*model.Component, error)
	PodsByViewSource(int) ([]*model.Pod, error)
	TopicByViewSource(int) (*model.Topic, error)
}

var _ generated.ViewSourceResolver = &ViewSourceResolver{}

// ViewSourceResolver resolves the view source's relationships
type ViewSourceResolver struct {
	*Resolver
}

// Component returns the view source's component
func (r *ViewSourceResolver) Component(ctx context.Context, viewSource *model.ViewSource) (*model.Component, error) {
	results, err := r.DataLoaders.ViewSourceLoader(ctx).ComponentByViewSource(viewSource.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get component from loader")
	}
	return results, err
}

// Pods returns the view source's pods
func (r *ViewSourceResolver) Pods(ctx context.Context, viewSource *model.ViewSource) ([]*model.Pod, error) {
	results, err := r.DataLoaders.ViewSourceLoader(ctx).PodsByViewSource(viewSource.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return results, err
}

// Topic returns the view source's topic
func (r *ViewSourceResolver) Topic(ctx context.Context, viewSource *model.ViewSource) (*model.Topic, error) {
	results, err := r.DataLoaders.ViewSourceLoader(ctx).TopicByViewSource(viewSource.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return results, err
}
