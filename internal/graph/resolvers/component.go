package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.ComponentLoader(ctx).ServiceByComponent(component.ID)
}

// Processors returns the component's processors
func (r *ComponentResolver) Processors(ctx context.Context, component *model.Component) ([]*model.Processor, error) {
	return r.DataLoaders.ComponentLoader(ctx).ProcessorsByComponent(component.ID)
}

// Sinks returns the component's sinks
func (r *ComponentResolver) Sinks(ctx context.Context, component *model.Component) ([]*model.Sink, error) {
	return r.DataLoaders.ComponentLoader(ctx).SinksByComponent(component.ID)
}

// Sources returns the component's sources
func (r *ComponentResolver) Sources(ctx context.Context, component *model.Component) ([]*model.Source, error) {
	return r.DataLoaders.ComponentLoader(ctx).SourcesByComponent(component.ID)
}

// ViewSinks returns the component's view sinks
func (r *ComponentResolver) ViewSinks(ctx context.Context, component *model.Component) ([]*model.ViewSink, error) {
	return r.DataLoaders.ComponentLoader(ctx).ViewSinksByComponent(component.ID)
}

// ViewSources returns the component's view sources
func (r *ComponentResolver) ViewSources(ctx context.Context, component *model.Component) ([]*model.ViewSource, error) {
	return r.DataLoaders.ComponentLoader(ctx).ViewSourcesByComponent(component.ID)
}

// Views returns the component's views
func (r *ComponentResolver) Views(ctx context.Context, component *model.Component) ([]*model.View, error) {
	return r.DataLoaders.ComponentLoader(ctx).ViewsByComponent(component.ID)
}
