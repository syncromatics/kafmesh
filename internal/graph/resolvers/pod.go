package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.PodResolver = &PodResolver{}

// PodResolver resolves a pod's relationships
type PodResolver struct {
	loaders loaderFunc
}

// Processors returns the pod's processors
func (r *PodResolver) Processors(ctx context.Context, pod *model.Pod) ([]*model.Processor, error) {
	return r.loaders(ctx).PodLoader.ProcessorsByPod.Load(pod.ID)
}

// Sinks returns the pod's sinks
func (r *PodResolver) Sinks(ctx context.Context, pod *model.Pod) ([]*model.Sink, error) {
	return r.loaders(ctx).PodLoader.SinksByPod.Load(pod.ID)
}

// Sources returns the pod's sources
func (r *PodResolver) Sources(ctx context.Context, pod *model.Pod) ([]*model.Source, error) {
	return r.loaders(ctx).PodLoader.SourcesByPod.Load(pod.ID)
}

// ViewSinks returns the pod's view sinks
func (r *PodResolver) ViewSinks(ctx context.Context, pod *model.Pod) ([]*model.ViewSink, error) {
	return r.loaders(ctx).PodLoader.ViewSinksByPod.Load(pod.ID)
}

// ViewSources returns the pod's view sources
func (r *PodResolver) ViewSources(ctx context.Context, pod *model.Pod) ([]*model.ViewSource, error) {
	return r.loaders(ctx).PodLoader.ViewSourcesByPod.Load(pod.ID)
}

// Views returns the pod's views
func (r *PodResolver) Views(ctx context.Context, pod *model.Pod) ([]*model.View, error) {
	return r.loaders(ctx).PodLoader.ViewsByPod.Load(pod.ID)
}
