package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./topics.go -destination=./topics_mock_test.go -package=loaders_test

// TopicRepository is the datastore repository for topics
type TopicRepository interface {
	ProcessorInputsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorInput, error)
	ProcessorJoinsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorJoin, error)
	ProcessorLookupsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorLookup, error)
	ProcessorOutputsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorOutput, error)
	ProcessorPersistencesByTopics(ctx context.Context, topics []int) ([][]*model.Processor, error)
	SinksByTopics(ctx context.Context, topics []int) ([][]*model.Sink, error)
	SourcesByTopics(ctx context.Context, topics []int) ([][]*model.Source, error)
	ViewSinksByTopics(ctx context.Context, topics []int) ([][]*model.ViewSink, error)
	ViewSourcesByTopics(ctx context.Context, topics []int) ([][]*model.ViewSource, error)
	ViewsByTopics(ctx context.Context, topics []int) ([][]*model.View, error)
}

var _ resolvers.TopicLoader = &TopicLoader{}

// TopicLoader contains data loaders for topic relationships
type TopicLoader struct {
	processorInputsByTopic       *generated.InputSliceLoader
	processorJoinsByTopic        *generated.JoinSliceLoader
	processorLookupsByTopic      *generated.LookupSliceLoader
	processorOutputsByTopic      *generated.OutputSliceLoader
	processorPersistencesByTopic *generated.ProcessorSliceLoader
	sinksByTopic                 *generated.SinkSliceLoader
	sourcesByTopic               *generated.SourceSliceLoader
	viewSinksByTopic             *generated.ViewSinkSliceLoader
	viewSourcesByTopic           *generated.ViewSourceSliceLoader
	viewsByTopic                 *generated.ViewSliceLoader
}

// NewTopicLoader creates a new TopicLoader
func NewTopicLoader(ctx context.Context, repository TopicRepository, waitTime time.Duration) *TopicLoader {
	loader := &TopicLoader{}

	loader.processorInputsByTopic = generated.NewInputSliceLoader(generated.InputSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorInput, []error) {
			r, err := repository.ProcessorInputsByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get inputs from repository")}
			}

			return r, nil
		},
	})

	loader.processorJoinsByTopic = generated.NewJoinSliceLoader(generated.JoinSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorJoin, []error) {
			r, err := repository.ProcessorJoinsByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get joins from repository")}
			}

			return r, nil
		},
	})

	loader.processorLookupsByTopic = generated.NewLookupSliceLoader(generated.LookupSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorLookup, []error) {
			r, err := repository.ProcessorLookupsByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get lookups from repository")}
			}

			return r, nil
		},
	})

	loader.processorOutputsByTopic = generated.NewOutputSliceLoader(generated.OutputSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ProcessorOutput, []error) {
			r, err := repository.ProcessorOutputsByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get outputs from repository")}
			}

			return r, nil
		},
	})

	loader.processorPersistencesByTopic = generated.NewProcessorSliceLoader(generated.ProcessorSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Processor, []error) {
			r, err := repository.ProcessorPersistencesByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get processors from repository")}
			}

			return r, nil
		},
	})

	loader.sinksByTopic = generated.NewSinkSliceLoader(generated.SinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Sink, []error) {
			r, err := repository.SinksByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sinks from repository")}
			}

			return r, nil
		},
	})

	loader.sourcesByTopic = generated.NewSourceSliceLoader(generated.SourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Source, []error) {
			r, err := repository.SourcesByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get sources from repository")}
			}

			return r, nil
		},
	})

	loader.viewSinksByTopic = generated.NewViewSinkSliceLoader(generated.ViewSinkSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			r, err := repository.ViewSinksByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sinks from repository")}
			}

			return r, nil
		},
	})

	loader.viewSourcesByTopic = generated.NewViewSourceSliceLoader(generated.ViewSourceSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			r, err := repository.ViewSourcesByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get view sources from repository")}
			}

			return r, nil
		},
	})

	loader.viewsByTopic = generated.NewViewSliceLoader(generated.ViewSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.View, []error) {
			r, err := repository.ViewsByTopics(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get views from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ProcessorInputsByTopic returns the processor inputs for the topic
func (l *TopicLoader) ProcessorInputsByTopic(topicID int) ([]*model.ProcessorInput, error) {
	return l.processorInputsByTopic.Load(topicID)
}

// ProcessorJoinsByTopic returns the processor joins for the topic
func (l *TopicLoader) ProcessorJoinsByTopic(topicID int) ([]*model.ProcessorJoin, error) {
	return l.processorJoinsByTopic.Load(topicID)
}

// ProcessorLookupsByTopic returns the processor lookups for the topic
func (l *TopicLoader) ProcessorLookupsByTopic(topicID int) ([]*model.ProcessorLookup, error) {
	return l.processorLookupsByTopic.Load(topicID)
}

// ProcessorOutputsByTopic returns the processor outputs for the topic
func (l *TopicLoader) ProcessorOutputsByTopic(topicID int) ([]*model.ProcessorOutput, error) {
	return l.processorOutputsByTopic.Load(topicID)
}

// ProcessorPersistencesByTopic returns the processor persistence for the topic
func (l *TopicLoader) ProcessorPersistencesByTopic(topicID int) ([]*model.Processor, error) {
	return l.processorPersistencesByTopic.Load(topicID)
}

// SinksByTopic returns the sinks for the topic
func (l *TopicLoader) SinksByTopic(topicID int) ([]*model.Sink, error) {
	return l.sinksByTopic.Load(topicID)
}

// SourcesByTopic returns the sources for the topic
func (l *TopicLoader) SourcesByTopic(topicID int) ([]*model.Source, error) {
	return l.sourcesByTopic.Load(topicID)
}

// ViewSinksByTopic returns the view sinks for the topic
func (l *TopicLoader) ViewSinksByTopic(topicID int) ([]*model.ViewSink, error) {
	return l.viewSinksByTopic.Load(topicID)
}

// ViewSourcesByTopic returns the view sources for the topic
func (l *TopicLoader) ViewSourcesByTopic(topicID int) ([]*model.ViewSource, error) {
	return l.viewSourcesByTopic.Load(topicID)
}

// ViewsByTopic returns the views for the topic
func (l *TopicLoader) ViewsByTopic(topicID int) ([]*model.View, error) {
	return l.viewsByTopic.Load(topicID)
}
