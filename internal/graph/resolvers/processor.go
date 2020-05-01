package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ProcessorLoader is the dataloaders for a processor
type ProcessorLoader interface {
	ComponentByProcessor(int) (*model.Component, error)
	InputsByProcessor(int) ([]*model.ProcessorInput, error)
	JoinsByProcessor(int) ([]*model.ProcessorJoin, error)
	LookupsByProcessor(int) ([]*model.ProcessorLookup, error)
	OutputsByProcessor(int) ([]*model.ProcessorOutput, error)
	PersistenceByProcessor(int) (*model.Topic, error)
	PodsByProcessor(int) ([]*model.Pod, error)
}

var _ generated.ProcessorResolver = &ProcessorResolver{}

// ProcessorResolver resolves the processor's relationships
type ProcessorResolver struct {
	*Resolver
}

// Component returns the processor's component
func (r *ProcessorResolver) Component(ctx context.Context, processor *model.Processor) (*model.Component, error) {
	return r.DataLoaders.ProcessorLoader(ctx).ComponentByProcessor(processor.ID)
}

// Inputs returns the processor's inputs
func (r *ProcessorResolver) Inputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorInput, error) {
	return r.DataLoaders.ProcessorLoader(ctx).InputsByProcessor(processor.ID)
}

// Joins returns the processor's joins
func (r *ProcessorResolver) Joins(ctx context.Context, processor *model.Processor) ([]*model.ProcessorJoin, error) {
	return r.DataLoaders.ProcessorLoader(ctx).JoinsByProcessor(processor.ID)
}

// Lookups returns the processor's lookups
func (r *ProcessorResolver) Lookups(ctx context.Context, processor *model.Processor) ([]*model.ProcessorLookup, error) {
	return r.DataLoaders.ProcessorLoader(ctx).LookupsByProcessor(processor.ID)
}

// Outputs returns the processor's outputs
func (r *ProcessorResolver) Outputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorOutput, error) {
	return r.DataLoaders.ProcessorLoader(ctx).OutputsByProcessor(processor.ID)
}

// Persistence returns the processor's peristence topic
func (r *ProcessorResolver) Persistence(ctx context.Context, processor *model.Processor) (*model.Topic, error) {
	return r.DataLoaders.ProcessorLoader(ctx).PersistenceByProcessor(processor.ID)
}

// Pods returns the processor's pods
func (r *ProcessorResolver) Pods(ctx context.Context, processor *model.Processor) ([]*model.Pod, error) {
	return r.DataLoaders.ProcessorLoader(ctx).PodsByProcessor(processor.ID)
}
