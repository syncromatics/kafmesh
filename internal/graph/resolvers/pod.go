package resolvers

import (
	"context"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

//go:generate mockgen -source=./pod.go -destination=./pod_mock_test.go -package=resolvers_test

// PodLoader is the dataloaders for a pod
type PodLoader interface {
	ProcessorsByPod(int) ([]*model.Processor, error)
	SinksByPod(int) ([]*model.Sink, error)
	SourcesByPod(int) ([]*model.Source, error)
	ViewSinksByPod(int) ([]*model.ViewSink, error)
	ViewSourcesByPod(int) ([]*model.ViewSource, error)
	ViewsByPod(int) ([]*model.View, error)
}

var _ generated.PodResolver = &PodResolver{}

// PodResolver resolves a pod's relationships
type PodResolver struct {
	*Resolver
}

// Processors returns the pod's processors
func (r *PodResolver) Processors(ctx context.Context, pod *model.Pod) ([]*model.Processor, error) {
	result, err := r.DataLoaders.PodLoader(ctx).ProcessorsByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processors from loader")
	}
	return result, nil
}

// Sinks returns the pod's sinks
func (r *PodResolver) Sinks(ctx context.Context, pod *model.Pod) ([]*model.Sink, error) {
	result, err := r.DataLoaders.PodLoader(ctx).SinksByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sinks from loader")
	}
	return result, nil
}

// Sources returns the pod's sources
func (r *PodResolver) Sources(ctx context.Context, pod *model.Pod) ([]*model.Source, error) {
	result, err := r.DataLoaders.PodLoader(ctx).SourcesByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sources from loader")
	}
	return result, nil
}

// ViewSinks returns the pod's view sinks
func (r *PodResolver) ViewSinks(ctx context.Context, pod *model.Pod) ([]*model.ViewSink, error) {
	result, err := r.DataLoaders.PodLoader(ctx).ViewSinksByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sinks from loader")
	}
	return result, nil
}

// ViewSources returns the pod's view sources
func (r *PodResolver) ViewSources(ctx context.Context, pod *model.Pod) ([]*model.ViewSource, error) {
	result, err := r.DataLoaders.PodLoader(ctx).ViewSourcesByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sources from loader")
	}
	return result, nil
}

// Views returns the pod's views
func (r *PodResolver) Views(ctx context.Context, pod *model.Pod) ([]*model.View, error) {
	result, err := r.DataLoaders.PodLoader(ctx).ViewsByPod(pod.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get views from loader")
	}
	return result, nil
}
