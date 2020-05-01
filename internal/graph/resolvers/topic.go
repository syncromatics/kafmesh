package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./topic.go -destination=./topic_mock_test.go -package=resolvers_test

// TopicLoader is the dataloader for a topic
type TopicLoader interface {
	ProcessorInputsByTopic(int) ([]*model.ProcessorInput, error)
	ProcessorJoinsByTopic(int) ([]*model.ProcessorJoin, error)
	ProcessorLookupsByTopic(int) ([]*model.ProcessorLookup, error)
	ProcessorOutputsByTopic(int) ([]*model.ProcessorOutput, error)
	ProcessorPersistencesByTopic(int) ([]*model.Processor, error)
	SinksByTopic(int) ([]*model.Sink, error)
	SourcesByTopic(int) ([]*model.Source, error)
	ViewSinksByTopic(int) ([]*model.ViewSink, error)
	ViewSourcesByTopic(int) ([]*model.ViewSource, error)
	ViewsByTopic(int) ([]*model.View, error)
}

var _ generated.TopicResolver = &TopicResolver{}

// TopicResolver resolvers the topic's relationships
type TopicResolver struct {
	*Resolver
}

// ProcessorInputs returns the topic's processor inputs
func (r *TopicResolver) ProcessorInputs(ctx context.Context, topic *model.Topic) ([]*model.ProcessorInput, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ProcessorInputsByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor inputs from loader")
	}
	return results, nil
}

// ProcessorJoins returns the topic's processor joins
func (r *TopicResolver) ProcessorJoins(ctx context.Context, topic *model.Topic) ([]*model.ProcessorJoin, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ProcessorJoinsByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor joins from loader")
	}
	return results, nil
}

// ProcessorLookups returns the topic's processor lookups
func (r *TopicResolver) ProcessorLookups(ctx context.Context, topic *model.Topic) ([]*model.ProcessorLookup, error) {
	result, err := r.DataLoaders.TopicLoader(ctx).ProcessorLookupsByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor lookups from loader")
	}
	return result, nil
}

// ProcessorOutputs returns the topic's processor outputs
func (r *TopicResolver) ProcessorOutputs(ctx context.Context, topic *model.Topic) ([]*model.ProcessorOutput, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ProcessorOutputsByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor outputs from loader")
	}
	return results, nil
}

// ProcessorPersistences returns the topic's processor persistences
func (r *TopicResolver) ProcessorPersistences(ctx context.Context, topic *model.Topic) ([]*model.Processor, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ProcessorPersistencesByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor persistences from loader")
	}
	return results, nil
}

// Sinks returns the topic's sinks
func (r *TopicResolver) Sinks(ctx context.Context, topic *model.Topic) ([]*model.Sink, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).SinksByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sinks from loader")
	}
	return results, nil
}

// Sources returns the topic's sources
func (r *TopicResolver) Sources(ctx context.Context, topic *model.Topic) ([]*model.Source, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).SourcesByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sources from loader")
	}
	return results, nil
}

// ViewSinks returns the topic's view sinks
func (r *TopicResolver) ViewSinks(ctx context.Context, topic *model.Topic) ([]*model.ViewSink, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ViewSinksByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sinks from loader")
	}
	return results, nil
}

// ViewSources returns the topic's view sources
func (r *TopicResolver) ViewSources(ctx context.Context, topic *model.Topic) ([]*model.ViewSource, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ViewSourcesByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sources from loader")
	}
	return results, nil
}

// Views returns the topic's views
func (r *TopicResolver) Views(ctx context.Context, topic *model.Topic) ([]*model.View, error) {
	results, err := r.DataLoaders.TopicLoader(ctx).ViewsByTopic(topic.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get views from loader")
	}
	return results, nil
}
