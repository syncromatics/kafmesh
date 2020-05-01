package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processor.go -destination=./processor_mock_test.go -package=resolvers_test

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
	result, err := r.DataLoaders.ProcessorLoader(ctx).ComponentByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get component from loader")
	}
	return result, nil
}

// Inputs returns the processor's inputs
func (r *ProcessorResolver) Inputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorInput, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).InputsByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get inputs from loader")
	}
	return result, nil
}

// Joins returns the processor's joins
func (r *ProcessorResolver) Joins(ctx context.Context, processor *model.Processor) ([]*model.ProcessorJoin, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).JoinsByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get joins from loader")
	}
	return result, nil
}

// Lookups returns the processor's lookups
func (r *ProcessorResolver) Lookups(ctx context.Context, processor *model.Processor) ([]*model.ProcessorLookup, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).LookupsByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get lookups from loader")
	}
	return result, nil
}

// Outputs returns the processor's outputs
func (r *ProcessorResolver) Outputs(ctx context.Context, processor *model.Processor) ([]*model.ProcessorOutput, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).OutputsByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get outputs from loader")
	}
	return result, nil
}

// Persistence returns the processor's peristence topic
func (r *ProcessorResolver) Persistence(ctx context.Context, processor *model.Processor) (*model.Topic, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).PersistenceByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get persistence from loader")
	}
	return result, nil
}

// Pods returns the processor's pods
func (r *ProcessorResolver) Pods(ctx context.Context, processor *model.Processor) ([]*model.Pod, error) {
	result, err := r.DataLoaders.ProcessorLoader(ctx).PodsByProcessor(processor.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return result, nil
}
