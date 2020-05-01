package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ProcessorInputRepository is the datastore repository for processor inputs
type ProcessorInputRepository interface {
	ProcessorByInputs(context.Context, []int) ([]*model.Processor, error)
	TopicByInputs(context.Context, []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorInputLoader = &ProcessorInputLoader{}

// ProcessorInputLoader contains data loaders for processor input relationships
type ProcessorInputLoader struct {
	processorByInput *processorLoader
	topicByInput     *topicLoader
}

// NewProcessorInputLoader creates a new processor inputs loader
func NewProcessorInputLoader(ctx context.Context, repository ProcessorInputRepository) *ProcessorInputLoader {
	loader := &ProcessorInputLoader{}

	loader.processorByInput = &processorLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByInputs(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.topicByInput = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByInputs(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	return loader
}

// ProcessorByInput returns the processor for the input
func (l *ProcessorInputLoader) ProcessorByInput(inputID int) (*model.Processor, error) {
	return l.processorByInput.Load(inputID)
}

// TopicByInput returns the topic for the input
func (l *ProcessorInputLoader) TopicByInput(inputID int) (*model.Topic, error) {
	return l.topicByInput.Load(inputID)
}
