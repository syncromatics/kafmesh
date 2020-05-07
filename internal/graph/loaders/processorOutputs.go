package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorOutputs.go -destination=./processorOutputs_mock_test.go -package=loaders_test

// ProcessorOutputRepository is the datastore repository for processor outputs
type ProcessorOutputRepository interface {
	ProcessorByOutputs(ctx context.Context, outputs []int) ([]*model.Processor, error)
	TopicByOutputs(ctx context.Context, outputs []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorOutputLoader = &ProcessorOutputLoader{}

// ProcessorOutputLoader contains data loaders for processor output relationships
type ProcessorOutputLoader struct {
	processorByOutput *generated.ProcessorLoader
	topicByOutput     *generated.TopicLoader
}

// NewProcessorOutputLoader creates a new ProcessorOutputLoader
func NewProcessorOutputLoader(ctx context.Context, repository ProcessorOutputRepository, waitTime time.Duration) *ProcessorOutputLoader {
	loader := &ProcessorOutputLoader{}

	loader.processorByOutput = generated.NewProcessorLoader(generated.ProcessorLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByOutputs(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processor from repository")}
			}
			return r, nil
		},
	})

	loader.topicByOutput = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByOutputs(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}

			return r, nil
		},
	})

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
