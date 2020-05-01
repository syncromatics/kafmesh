package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

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
	componentByProcessor   *componentLoader
	inputsByProcessor      *inputSliceLoader
	joinsByProcessor       *joinSliceLoader
	lookupsByProcessor     *lookupSliceLoader
	outputsByProcessor     *outputSliceLoader
	podsByProcessor        *podSliceLoader
	persistenceByProcessor *topicLoader
}

// NewProcessorLoader creates a new ProcessorLoader
func NewProcessorLoader(ctx context.Context, repository ProcessorRepository) *ProcessorLoader {
	loader := &ProcessorLoader{}

	loader.componentByProcessor = &componentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			r, err := repository.ComponentByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.inputsByProcessor = &inputSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorInput, []error) {
			r, err := repository.InputsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.joinsByProcessor = &joinSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorJoin, []error) {
			r, err := repository.JoinsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.lookupsByProcessor = &lookupSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorLookup, []error) {
			r, err := repository.LookupsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.outputsByProcessor = &outputSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorOutput, []error) {
			r, err := repository.OutputsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.podsByProcessor = &podSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			r, err := repository.PodsByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	loader.persistenceByProcessor = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.PersistenceByProcessors(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

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
