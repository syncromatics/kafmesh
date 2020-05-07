package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorLookups.go -destination=./processorLookups_mock_test.go -package=loaders_test

// ProcessorLookupRepository is the datastore repository for processor lookups
type ProcessorLookupRepository interface {
	ProcessorByLookups(ctx context.Context, lookups []int) ([]*model.Processor, error)
	TopicByLookups(ctx context.Context, lookups []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorLookupLoader = &ProcessorLookupLoader{}

// ProcessorLookupLoader contains data loaders for processor lookup relationships
type ProcessorLookupLoader struct {
	processorByLookup *generated.ProcessorLoader
	topicByLookup     *generated.TopicLoader
}

// NewProcessorLookupLoader creates a new ProcessorLookupLoader
func NewProcessorLookupLoader(ctx context.Context, repository ProcessorLookupRepository, waitTime time.Duration) *ProcessorLookupLoader {
	loader := &ProcessorLookupLoader{}

	loader.processorByLookup = generated.NewProcessorLoader(generated.ProcessorLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByLookups(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processor from repository")}
			}
			return r, nil
		},
	})

	loader.topicByLookup = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByLookups(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}
			return r, nil
		},
	})

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
