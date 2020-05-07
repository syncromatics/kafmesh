package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorInputs.go -destination=./processorInputs_mock_test.go -package=loaders_test

// ProcessorInputRepository is the datastore repository for processor inputs
type ProcessorInputRepository interface {
	ProcessorByInputs(context.Context, []int) ([]*model.Processor, error)
	TopicByInputs(context.Context, []int) ([]*model.Topic, error)
}

var _ resolvers.ProcessorInputLoader = &ProcessorInputLoader{}

// ProcessorInputLoader contains data loaders for processor input relationships
type ProcessorInputLoader struct {
	processorByInput *generated.ProcessorLoader
	topicByInput     *generated.TopicLoader
}

// NewProcessorInputLoader creates a new processor inputs loader
func NewProcessorInputLoader(ctx context.Context, repository ProcessorInputRepository, waitTime time.Duration) *ProcessorInputLoader {
	loader := &ProcessorInputLoader{}

	loader.processorByInput = generated.NewProcessorLoader(generated.ProcessorLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Processor, []error) {
			r, err := repository.ProcessorByInputs(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processor from repository")}
			}
			return r, nil
		},
	})

	loader.topicByInput = generated.NewTopicLoader(generated.TopicLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([]*model.Topic, []error) {
			r, err := repository.TopicByInputs(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get topic from repository")}
			}
			return r, nil
		},
	})

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
