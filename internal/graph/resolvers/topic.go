package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.TopicLoader(ctx).ProcessorInputsByTopic(topic.ID)
}

// ProcessorJoins returns the topic's processor joins
func (r *TopicResolver) ProcessorJoins(ctx context.Context, topic *model.Topic) ([]*model.ProcessorJoin, error) {
	return r.DataLoaders.TopicLoader(ctx).ProcessorJoinsByTopic(topic.ID)
}

// ProcessorLookups returns the topic's processor lookups
func (r *TopicResolver) ProcessorLookups(ctx context.Context, topic *model.Topic) ([]*model.ProcessorLookup, error) {
	return r.DataLoaders.TopicLoader(ctx).ProcessorLookupsByTopic(topic.ID)
}

// ProcessorOutputs returns the topic's processor outputs
func (r *TopicResolver) ProcessorOutputs(ctx context.Context, topic *model.Topic) ([]*model.ProcessorOutput, error) {
	return r.DataLoaders.TopicLoader(ctx).ProcessorOutputsByTopic(topic.ID)
}

// ProcessorPersistences returns the topic's processor persistences
func (r *TopicResolver) ProcessorPersistences(ctx context.Context, topic *model.Topic) ([]*model.Processor, error) {
	return r.DataLoaders.TopicLoader(ctx).ProcessorPersistencesByTopic(topic.ID)
}

// Sinks returns the topic's sinks
func (r *TopicResolver) Sinks(ctx context.Context, topic *model.Topic) ([]*model.Sink, error) {
	return r.DataLoaders.TopicLoader(ctx).SinksByTopic(topic.ID)
}

// Sources returns the topic's sources
func (r *TopicResolver) Sources(ctx context.Context, topic *model.Topic) ([]*model.Source, error) {
	return r.DataLoaders.TopicLoader(ctx).SourcesByTopic(topic.ID)
}

// ViewSinks returns the topic's view sinks
func (r *TopicResolver) ViewSinks(ctx context.Context, topic *model.Topic) ([]*model.ViewSink, error) {
	return r.DataLoaders.TopicLoader(ctx).ViewSinksByTopic(topic.ID)
}

// ViewSources returns the topic's view sources
func (r *TopicResolver) ViewSources(ctx context.Context, topic *model.Topic) ([]*model.ViewSource, error) {
	return r.DataLoaders.TopicLoader(ctx).ViewSourcesByTopic(topic.ID)
}

// Views returns the topic's views
func (r *TopicResolver) Views(ctx context.Context, topic *model.Topic) ([]*model.View, error) {
	return r.DataLoaders.TopicLoader(ctx).ViewsByTopic(topic.ID)
}
