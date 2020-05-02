package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./pods.go -destination=./pods_mock_test.go -package=loaders_test

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
	processorsByPod  *generated.ProcessorSliceLoader
	sinksByPod       *generated.SinkSliceLoader
	sourcesByPod     *generated.SourceSliceLoader
	viewSinksByPod   *generated.ViewSinkSliceLoader
	viewSourcesByPod *generated.ViewSourceSliceLoader
	viewsByPod       *generated.ViewSliceLoader
}

// NewPodLoader creates a new pod data loader
func NewPodLoader(ctx context.Context, repository PodRepository, waitTime time.Duration) *PodLoader {
	loader := &PodLoader{}

	loader.processorsByPod = generated.NewProcessorSliceLoader(generated.ProcessorSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Processor, []error) {
			r, err := repository.ProcessorsByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processors from repository")}
			}
			return r, nil
		},
	})

	loader.sinksByPod = generated.NewSinkSliceLoader(generated.SinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Sink, []error) {
			r, err := repository.SinksByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sinks from repository")}
			}
			return r, nil
		},
	})

	loader.sourcesByPod = generated.NewSourceSliceLoader(generated.SourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Source, []error) {
			r, err := repository.SourcesByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sources from repository")}
			}
			return r, nil
		},
	})

	loader.viewSinksByPod = generated.NewViewSinkSliceLoader(generated.ViewSinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			r, err := repository.ViewSinksByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sinks from repository")}
			}
			return r, nil
		},
	})

	loader.viewSourcesByPod = generated.NewViewSourceSliceLoader(generated.ViewSourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			r, err := repository.ViewSourcesByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sources from repository")}
			}
			return r, nil
		},
	})

	loader.viewsByPod = generated.NewViewSliceLoader(generated.ViewSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.View, []error) {
			r, err := repository.ViewsByPods(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get views from repository")}
			}
			return r, nil
		},
	})

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
