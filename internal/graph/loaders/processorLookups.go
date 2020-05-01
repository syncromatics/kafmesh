package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ProcessorLookupRepository is the datastore repository for processor lookups
type ProcessorLookupRepository interface {
	ProcessorByLookups(ctx context.Context, lookups []int) ([]*model.Processor, error)
	TopicByLookups(ctx context.Context, lookups []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorLookupLoader = &ProcessorLookupLoader{}

// ProcessorLookupLoader contains data loaders for processor lookup relationships
type ProcessorLookupLoader struct {
	processorByLookup *processorLoader
	topicByLookup     *topicLoader
}

// NewProcessorLookupLoader creates a new ProcessorLookupLoader
func NewProcessorLookupLoader(ctx context.Context, repository ProcessorLookupRepository) *ProcessorLookupLoader {
	loader := &ProcessorLookupLoader{}

	loader.processorByLookup = &processorLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByLookups(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.topicByLookup = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByLookups(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	return loader
}

// ProcessorByLookup returns the processor for the lookup
func (l *ProcessorLookupLoader) ProcessorByLookup(lookupID int) (*model.Processor, error) {
	return l.processorByLookup.Load(lookupID)
}

// TopicByLookup returns the topic for the lookup
func (l *ProcessorLookupLoader) TopicByLookup(lookupID int) (*model.Topic, error) {
	return l.topicByLookup.Load(lookupID)
}
