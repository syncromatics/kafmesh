package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ComponentResolver = &ComponentResolver{}

// ComponentResolver resolves component relationships
type ComponentResolver struct {
	loader loaderFunc
}

// Service returns the component's service
func (r *ComponentResolver) Service(ctx context.Context, component *model.Component) (*model.Service, error) {
	return r.loader(ctx).ComponentLoader.ServiceByComponent.Load(component.ID)
}

// Processors returns the component's processors
func (r *ComponentResolver) Processors(ctx context.Context, component *model.Component) ([]*model.Processor, error) {
	return r.loader(ctx).ComponentLoader.ProcessorsByComponent.Load(component.ID)
}

// Sinks returns the component's sinks
func (r *ComponentResolver) Sinks(ctx context.Context, component *model.Component) ([]*model.Sink, error) {
	return r.loader(ctx).ComponentLoader.SinksByComponent.Load(component.ID)
}

// Sources returns the component's sources
func (r *ComponentResolver) Sources(ctx context.Context, component *model.Component) ([]*model.Source, error) {
	return r.loader(ctx).ComponentLoader.SourcesByComponent.Load(component.ID)
}

// ViewSinks returns the component's view sinks
func (r *ComponentResolver) ViewSinks(ctx context.Context, component *model.Component) ([]*model.ViewSink, error) {
	return r.loader(ctx).ComponentLoader.ViewSinksByComponent.Load(component.ID)
}

// ViewSources returns the component's view sources
func (r *ComponentResolver) ViewSources(ctx context.Context, component *model.Component) ([]*model.ViewSource, error) {
	return r.loader(ctx).ComponentLoader.ViewSourcesByComponent.Load(component.ID)
}

// Views returns the component's views
func (r *ComponentResolver) Views(ctx context.Context, component *model.Component) ([]*model.View, error) {
	return r.loader(ctx).ComponentLoader.ViewsByComponent.Load(component.ID)
}
