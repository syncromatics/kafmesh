package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processors.go -destination=./processors_mock_test.go -package=loaders_test

// ProcessorRepository is the datastore repository for processors
type ProcessorRepository interface {
	ComponentByProcessors(ctx context.Context, processors []int) ([]*model.Component, error)
	InputsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorInput, error)
	JoinsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorJoin, error)
	LookupsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorLookup, error)
	OutputsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorOutput, error)
	PodsByProcessors(ctx context.Context, processors []int) ([][]*model.Pod, error)
	PersistenceByProcessors(ctx context.Context, processors []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorLoader = &ProcessorLoader{}

// ProcessorLoader contains data loaders for processor relationships
type ProcessorLoader struct {
	componentByProcessor   *generated.ComponentLoader
	inputsByProcessor      *generated.InputSliceLoader
	joinsByProcessor       *generated.JoinSliceLoader
	lookupsByProcessor     *generated.LookupSliceLoader
	outputsByProcessor     *generated.OutputSliceLoader
	podsByProcessor        *generated.PodSliceLoader
	persistenceByProcessor *generated.TopicLoader
}

// NewProcessorLoader creates a new ProcessorLoader
func NewProcessorLoader(ctx context.Context, repository ProcessorRepository, waitTime time.Duration) *ProcessorLoader {
	loader := &ProcessorLoader{}

	loader.componentByProcessor = generated.NewComponentLoader(generated.ComponentLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get component from repository")}
			}

			return r, nil
		},
	})

	loader.inputsByProcessor = generated.NewInputSliceLoader(generated.InputSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorInput, []error) {
			r, err := repository.InputsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get inputs from repository")}
			}

			return r, nil
		},
	})

	loader.joinsByProcessor = generated.NewJoinSliceLoader(generated.JoinSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorJoin, []error) {
			r, err := repository.JoinsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get joins from repository")}
			}

			return r, nil
		},
	})

	loader.lookupsByProcessor = generated.NewLookupSliceLoader(generated.LookupSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorLookup, []error) {
			r, err := repository.LookupsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get lookups from repository")}
			}

			return r, nil
		},
	})

	loader.outputsByProcessor = generated.NewOutputSliceLoader(generated.OutputSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorOutput, []error) {
			r, err := repository.OutputsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get outputs from repository")}
			}

			return r, nil
		},
	})

	loader.podsByProcessor = generated.NewPodSliceLoader(generated.PodSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get pods from repository")}
			}

			return r, nil
		},
	})

	loader.persistenceByProcessor = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.PersistenceByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get persistence from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ComponentByProcessor returns the component for a processor
func (l *ProcessorLoader) ComponentByProcessor(processorID int) (*model.Component, error) {
	return l.componentByProcessor.Load(processorID)
}

// InputsByProcessor returns the inputs for a processor
func (l *ProcessorLoader) InputsByProcessor(processorID int) ([]*model.ProcessorInput, error) {
	return l.inputsByProcessor.Load(processorID)
}

// JoinsByProcessor returns the joins for a processor
func (l *ProcessorLoader) JoinsByProcessor(processorID int) ([]*model.ProcessorJoin, error) {
	return l.joinsByProcessor.Load(processorID)
}

// LookupsByProcessor returns the lookups for a processor
func (l *ProcessorLoader) LookupsByProcessor(processorID int) ([]*model.ProcessorLookup, error) {
	return l.lookupsByProcessor.Load(processorID)
}

// OutputsByProcessor returns the outputs for a processor
func (l *ProcessorLoader) OutputsByProcessor(processorID int) ([]*model.ProcessorOutput, error) {
	return l.outputsByProcessor.Load(processorID)
}

// PodsByProcessor returns the pods for a processor
func (l *ProcessorLoader) PodsByProcessor(processorID int) ([]*model.Pod, error) {
	return l.podsByProcessor.Load(processorID)
}

// PersistenceByProcessor returns the perisistence topic for a processor
func (l *ProcessorLoader) PersistenceByProcessor(processorID int) (*model.Topic, error) {
	return l.persistenceByProcessor.Load(processorID)
}
