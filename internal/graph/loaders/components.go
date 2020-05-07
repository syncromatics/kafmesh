package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./components.go -destination=./components_mock_test.go -package=loaders_test

// ComponentRepository is the datastore repository for components
type ComponentRepository interface {
	ServicesByComponents(ctx context.Context, components []int) ([]*model.Service, error)
	ProcessorsByComponents(ctx context.Context, components []int) ([][]*model.Processor, error)
	SinksByComponents(ctx context.Context, components []int) ([][]*model.Sink, error)
	SourcesByComponents(ctx context.Context, components []int) ([][]*model.Source, error)
	ViewSinksByComponents(ctx context.Context, components []int) ([][]*model.ViewSink, error)
	ViewSourcesByComponents(ctx context.Context, components []int) ([][]*model.ViewSource, error)
	ViewsByComponents(ctx context.Context, components []int) ([][]*model.View, error)
}

var _ resolvers.ComponentLoader = &ComponentLoader{}

// ComponentLoader is the dataloader for component relationships
type ComponentLoader struct {
	serviceByComponent     *generated.ServiceLoader
	processorsByComponent  *generated.ProcessorSliceLoader
	sinksByComponent       *generated.SinkSliceLoader
	sourcesByComponent     *generated.SourceSliceLoader
	viewSinksByComponent   *generated.ViewSinkSliceLoader
	viewSourcesByComponent *generated.ViewSourceSliceLoader
	viewsByComponent       *generated.ViewSliceLoader
}

// NewComponentLoader creates a new component dataloader
func NewComponentLoader(ctx context.Context, repository ComponentRepository, waitTime time.Duration) *ComponentLoader {
	loader := &ComponentLoader{}
	loader.serviceByComponent = generated.NewServiceLoader(generated.ServiceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Service, []error) {
			r, err := repository.ServicesByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get services from repository")}
			}
			return r, nil
		},
	})

	loader.processorsByComponent = generated.NewProcessorSliceLoader(generated.ProcessorSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Processor, []error) {
			r, err := repository.ProcessorsByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processors from repository")}
			}
			return r, nil
		},
	})

	loader.sinksByComponent = generated.NewSinkSliceLoader(generated.SinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Sink, []error) {
			r, err := repository.SinksByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sinks from repository")}
			}
			return r, nil
		},
	})

	loader.sourcesByComponent = generated.NewSourceSliceLoader(generated.SourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Source, []error) {
			r, err := repository.SourcesByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sources from repository")}
			}
			return r, nil
		},
	})

	loader.viewSinksByComponent = generated.NewViewSinkSliceLoader(generated.ViewSinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			r, err := repository.ViewSinksByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sinks from repository")}
			}
			return r, nil
		},
	})

	loader.viewSourcesByComponent = generated.NewViewSourceSliceLoader(generated.ViewSourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			r, err := repository.ViewSourcesByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sources from repository")}
			}
			return r, nil
		},
	})

	loader.viewsByComponent = generated.NewViewSliceLoader(generated.ViewSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.View, []error) {
			r, err := repository.ViewsByComponents(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get views from repository")}
			}
			return r, nil
		},
	})
	return loader
}

// ProcessorsByComponent returns the processors for the component
func (c *ComponentLoader) ProcessorsByComponent(componentID int) ([]*model.Processor, error) {
	return c.processorsByComponent.Load(componentID)
}

// ServiceByComponent returns the component's service
func (c *ComponentLoader) ServiceByComponent(componentID int) (*model.Service, error) {
	return c.serviceByComponent.Load(componentID)
}

// SinksByComponent returns the sinks for the component
func (c *ComponentLoader) SinksByComponent(componentID int) ([]*model.Sink, error) {
	return c.sinksByComponent.Load(componentID)
}

// SourcesByComponent returns the sources for the component
func (c *ComponentLoader) SourcesByComponent(componentID int) ([]*model.Source, error) {
	return c.sourcesByComponent.Load(componentID)
}

// ViewSinksByComponent returns the view sinks for the component
func (c *ComponentLoader) ViewSinksByComponent(componentID int) ([]*model.ViewSink, error) {
	return c.viewSinksByComponent.Load(componentID)
}

// ViewSourcesByComponent returns the view sources for the component
func (c *ComponentLoader) ViewSourcesByComponent(componentID int) ([]*model.ViewSource, error) {
	return c.viewSourcesByComponent.Load(componentID)
}

// ViewsByComponent returns the views for the components
func (c *ComponentLoader) ViewsByComponent(componentID int) ([]*model.View, error) {
	return c.viewsByComponent.Load(componentID)
}
