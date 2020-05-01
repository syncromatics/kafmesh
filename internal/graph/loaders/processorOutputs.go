package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ProcessorOutputRepository is the datastore repository for processor outputs
type ProcessorOutputRepository interface {
	ProcessorByOutputs(ctx context.Context, outputs []int) ([]*model.Processor, error)
	TopicByOutputs(ctx context.Context, outputs []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorOutputLoader = &ProcessorOutputLoader{}

// ProcessorOutputLoader contains data loaders for processor output relationships
type ProcessorOutputLoader struct {
	processorByOutput *processorLoader
	topicByOutput     *topicLoader
}

// NewProcessorOutputLoader creates a new ProcessorOutputLoader
func NewProcessorOutputLoader(ctx context.Context, repository ProcessorOutputRepository) *ProcessorOutputLoader {
	loader := &ProcessorOutputLoader{}

	loader.processorByOutput = &processorLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByOutputs(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.topicByOutput = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByOutputs(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	return loader
}

// ProcessorByOutput returns the processor for an output
func (l *ProcessorOutputLoader) ProcessorByOutput(outputID int) (*model.Processor, error) {
	return l.processorByOutput.Load(outputID)
}

// TopicByOutput returns the topic for an output
func (l *ProcessorOutputLoader) TopicByOutput(outputID int) (*model.Topic, error) {
	return l.topicByOutput.Load(outputID)
}
