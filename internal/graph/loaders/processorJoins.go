package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorJoins.go -destination=./processorJoins_mock_test.go -package=loaders_test

// ProcessorJoinRepository is the datastore respository for processor joins
type ProcessorJoinRepository interface {
	ProcessorByJoins(ctx context.Context, joins []int) ([]*model.Processor, error)
	TopicByJoins(ctx context.Context, joins []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorJoinLoader = &ProcessorJoinLoader{}

// ProcessorJoinLoader contains data loaders for processor join relationships
type ProcessorJoinLoader struct {
	processorByJoin *generated.ProcessorLoader
	topicByJoin     *generated.TopicLoader
}

// NewProcessorJoinLoader creates a new ProcessorJoinLoader
func NewProcessorJoinLoader(ctx context.Context, repository ProcessorJoinRepository, waitTime time.Duration) *ProcessorJoinLoader {
	loader := &ProcessorJoinLoader{}

	loader.processorByJoin = generated.NewProcessorLoader(generated.ProcessorLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByJoins(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processor from repository")}
			}
			return r, nil
		},
	})

	loader.topicByJoin = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByJoins(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}
			return r, nil
		},
	})

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
