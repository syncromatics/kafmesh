package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.PodLoader(ctx).ProcessorsByPod(pod.ID)
}

// Sinks returns the pod's sinks
func (r *PodResolver) Sinks(ctx context.Context, pod *model.Pod) ([]*model.Sink, error) {
	return r.DataLoaders.PodLoader(ctx).SinksByPod(pod.ID)
}

// Sources returns the pod's sources
func (r *PodResolver) Sources(ctx context.Context, pod *model.Pod) ([]*model.Source, error) {
	return r.DataLoaders.PodLoader(ctx).SourcesByPod(pod.ID)
}

// ViewSinks returns the pod's view sinks
func (r *PodResolver) ViewSinks(ctx context.Context, pod *model.Pod) ([]*model.ViewSink, error) {
	return r.DataLoaders.PodLoader(ctx).ViewSinksByPod(pod.ID)
}

// ViewSources returns the pod's view sources
func (r *PodResolver) ViewSources(ctx context.Context, pod *model.Pod) ([]*model.ViewSource, error) {
	return r.DataLoaders.PodLoader(ctx).ViewSourcesByPod(pod.ID)
}

// Views returns the pod's views
func (r *PodResolver) Views(ctx context.Context, pod *model.Pod) ([]*model.View, error) {
	return r.DataLoaders.PodLoader(ctx).ViewsByPod(pod.ID)
}
