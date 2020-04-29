package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ProcessorResolver = &ProcessorResolver{}

// ProcessorResolver resolves the processor's relationships
type ProcessorResolver struct {
	loader loaderFunc
}

// Component returns the processor's component
func (r *ProcessorResolver) Component(ctx context.Context, processor *model.Processor) (*model.Component, error) {
	return r.loader(ctx).ProcessorLoader.ComponentByProcessor.Load(processor.ID)
}

// Inputs returns the processor's inputs
func (r *ProcessorResolver) Inputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorInput, error) {
	return r.loader(ctx).ProcessorLoader.InputsByProcessor.Load(processor.ID)
}

// Joins returns the processor's joins
func (r *ProcessorResolver) Joins(ctx context.Context, processor *model.Processor) ([]*model.ProcessorJoin, error) {
	return r.loader(ctx).ProcessorLoader.JoinsByProcessor.Load(processor.ID)
}

// Lookups returns the processor's lookups
func (r *ProcessorResolver) Lookups(ctx context.Context, processor *model.Processor) ([]*model.ProcessorLookup, error) {
	return r.loader(ctx).ProcessorLoader.LookupsByProcessor.Load(processor.ID)
}

// Outputs returns the processor's outputs
func (r *ProcessorResolver) Outputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorOutput, error) {
	return r.loader(ctx).ProcessorLoader.OutputsByProcessor.Load(processor.ID)
}

// Persistence returns the processor's peristence topic
func (r *ProcessorResolver) Persistence(ctx context.Context, processor *model.Processor) (*model.Topic, error) {
	return r.loader(ctx).ProcessorLoader.PersistenceByProcessor.Load(processor.ID)
}

// Pods returns the processor's pods
func (r *ProcessorResolver) Pods(ctx context.Context, processor *model.Processor) ([]*model.Pod, error) {
	return r.loader(ctx).ProcessorLoader.PodsByProcessor.Load(processor.ID)
}
