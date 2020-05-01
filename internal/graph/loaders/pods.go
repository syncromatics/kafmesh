package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// PodRepository is the datastore repository for pods
type PodRepository interface {
	ProcessorsByPods(context.Context, []int) ([][]*model.Processor, error)
	SinksByPods(context.Context, []int) ([][]*model.Sink, error)
	SourcesByPods(ctx context.Context, pods []int) ([][]*model.Source, error)
	ViewSinksByPods(ctx context.Context, pods []int) ([][]*model.ViewSink, error)
	ViewSourcesByPods(ctx context.Context, pods []int) ([][]*model.ViewSource, error)
	ViewsByPods(ctx context.Context, pods []int) ([][]*model.View, error)
}

var _ resolvers.PodLoader = &PodLoader{}

// PodLoader contains data loaders for pod relationships
type PodLoader struct {
	processorsByPod  *processorSliceLoader
	sinksByPod       *sinkSliceLoader
	sourcesByPod     *sourceSliceLoader
	viewSinksByPod   *viewSinkSliceLoader
	viewSourcesByPod *viewSourceSliceLoader
	viewsByPod       *viewSliceLoader
}

// NewPodLoader creates a new pod data loader
func NewPodLoader(ctx context.Context, repository PodRepository) *PodLoader {
	loader := &PodLoader{}

	loader.processorsByPod = &processorSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Processor, []error) {
			r, err := repository.ProcessorsByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.sinksByPod = &sinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Sink, []error) {
			r, err := repository.SinksByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.sourcesByPod = &sourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Source, []error) {
			r, err := repository.SourcesByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.viewSinksByPod = &viewSinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			r, err := repository.ViewSinksByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.viewSourcesByPod = &viewSourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			r, err := repository.ViewSourcesByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.viewsByPod = &viewSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.View, []error) {
			r, err := repository.ViewsByPods(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	return loader
}

// ProcessorsByPod returns the processors for the pod
func (l *PodLoader) ProcessorsByPod(podID int) ([]*model.Processor, error) {
	return l.processorsByPod.Load(podID)
}

// SinksByPod returns the sinks for the pod
func (l *PodLoader) SinksByPod(podID int) ([]*model.Sink, error) {
	return l.sinksByPod.Load(podID)
}

// SourcesByPod returns the sources for the pod
func (l *PodLoader) SourcesByPod(podID int) ([]*model.Source, error) {
	return l.sourcesByPod.Load(podID)
}

// ViewSinksByPod returns the view sinks for the pod
func (l *PodLoader) ViewSinksByPod(podID int) ([]*model.ViewSink, error) {
	return l.viewSinksByPod.Load(podID)
}

// ViewSourcesByPod returns the view sources for the pod
func (l *PodLoader) ViewSourcesByPod(podID int) ([]*model.ViewSource, error) {
	return l.viewSourcesByPod.Load(podID)
}

// ViewsByPod returns the views for the pod
func (l *PodLoader) ViewsByPod(podID int) ([]*model.View, error) {
	return l.viewsByPod.Load(podID)
}
