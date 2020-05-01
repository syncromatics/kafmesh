package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./sink.go -destination=./sink_mock_test.go -package=resolvers_test

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
	results, err := r.DataLoaders.SinkLoader(ctx).ComponentBySink(sink.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get component from loader")
	}
	return results, nil
}

// Pods returns the sink's pods
func (r *SinkResolver) Pods(ctx context.Context, sink *model.Sink) ([]*model.Pod, error) {
	results, err := r.DataLoaders.SinkLoader(ctx).PodsBySink(sink.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return results, nil
}

// Topic returns the sink's topic
func (r *SinkResolver) Topic(ctx context.Context, sink *model.Sink) (*model.Topic, error) {
	results, err := r.DataLoaders.SinkLoader(ctx).TopicBySink(sink.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return results, nil
}
