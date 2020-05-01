package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./component.go -destination=./component_mock_test.go -package=resolvers_test

// ComponentLoader is the dataloader for components
type ComponentLoader interface {
	ServiceByComponent(int) (*model.Service, error)
	ProcessorsByComponent(int) ([]*model.Processor, error)
	SinksByComponent(int) ([]*model.Sink, error)
	SourcesByComponent(int) ([]*model.Source, error)
	ViewSinksByComponent(int) ([]*model.ViewSink, error)
	ViewSourcesByComponent(int) ([]*model.ViewSource, error)
	ViewsByComponent(int) ([]*model.View, error)
}

var _ generated.ComponentResolver = &ComponentResolver{}

// ComponentResolver resolves component relationships
type ComponentResolver struct {
	*Resolver
}

// Service returns the component's service
func (r *ComponentResolver) Service(ctx context.Context, component *model.Component) (*model.Service, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).ServiceByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get service from loader")
	}
	return result, nil
}

// Processors returns the component's processors
func (r *ComponentResolver) Processors(ctx context.Context, component *model.Component) ([]*model.Processor, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).ProcessorsByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processors from loader")
	}
	return result, nil
}

// Sinks returns the component's sinks
func (r *ComponentResolver) Sinks(ctx context.Context, component *model.Component) ([]*model.Sink, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).SinksByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sinks from loader")
	}
	return result, nil
}

// Sources returns the component's sources
func (r *ComponentResolver) Sources(ctx context.Context, component *model.Component) ([]*model.Source, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).SourcesByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sources from loader")
	}
	return result, nil
}

// ViewSinks returns the component's view sinks
func (r *ComponentResolver) ViewSinks(ctx context.Context, component *model.Component) ([]*model.ViewSink, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).ViewSinksByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sinks from loader")
	}
	return result, nil
}

// ViewSources returns the component's view sources
func (r *ComponentResolver) ViewSources(ctx context.Context, component *model.Component) ([]*model.ViewSource, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).ViewSourcesByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sources from loader")
	}
	return result, nil
}

// Views returns the component's views
func (r *ComponentResolver) Views(ctx context.Context, component *model.Component) ([]*model.View, error) {
	result, err := r.DataLoaders.ComponentLoader(ctx).ViewsByComponent(component.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get views from loader")
	}
	return result, nil
}
