package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ProcessorJoinRepository is the datastore respository for processor joins
type ProcessorJoinRepository interface {
	ProcessorByJoins(ctx context.Context, joins []int) ([]*model.Processor, error)
	TopicByJoins(ctx context.Context, joins []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorJoinLoader = &ProcessorJoinLoader{}

// ProcessorJoinLoader contains data loaders for processor join relationships
type ProcessorJoinLoader struct {
	processorByJoin *processorLoader
	topicByJoin     *topicLoader
}

// NewProcessorJoinLoader creates a new ProcessorJoinLoader
func NewProcessorJoinLoader(ctx context.Context, repository ProcessorJoinRepository) *ProcessorJoinLoader {
	loader := &ProcessorJoinLoader{}

	loader.processorByJoin = &processorLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByJoins(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	loader.topicByJoin = &topicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByJoins(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}
			return r, nil
		},
	}

	return loader
}

// ProcessorByJoin returns the processor for the join
func (l *ProcessorJoinLoader) ProcessorByJoin(joinID int) (*model.Processor, error) {
	return l.processorByJoin.Load(joinID)
}

// TopicByJoin returns the topic for the join
func (l *ProcessorJoinLoader) TopicByJoin(joinID int) (*model.Topic, error) {
	return l.topicByJoin.Load(joinID)
}
