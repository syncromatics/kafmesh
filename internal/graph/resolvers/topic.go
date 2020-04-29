package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.TopicResolver = &TopicResolver{}

// TopicResolver resolvers the topic's relationships
type TopicResolver struct {
	loaderFunc loaderFunc
}

// ProcessorInputs returns the topic's processor inputs
func (r *TopicResolver) ProcessorInputs(ctx context.Context, topic *model.Topic) ([]*model.ProcessorInput, error) {
	return r.loaderFunc(ctx).TopicLoader.ProcessorInputsByTopic.Load(topic.ID)
}

// ProcessorJoins returns the topic's processor joins
func (r *TopicResolver) ProcessorJoins(ctx context.Context, topic *model.Topic) ([]*model.ProcessorJoin, error) {
	return r.loaderFunc(ctx).TopicLoader.ProcessorJoinsByTopic.Load(topic.ID)
}

// ProcessorLookups returns the topic's processor lookups
func (r *TopicResolver) ProcessorLookups(ctx context.Context, topic *model.Topic) ([]*model.ProcessorLookup, error) {
	return r.loaderFunc(ctx).TopicLoader.ProcessorLookupsByTopic.Load(topic.ID)
}

// ProcessorOutputs returns the topic's processor outputs
func (r *TopicResolver) ProcessorOutputs(ctx context.Context, topic *model.Topic) ([]*model.ProcessorOutput, error) {
	return r.loaderFunc(ctx).TopicLoader.ProcessorOutputsByTopic.Load(topic.ID)
}

// ProcessorPersistences returns the topic's processor persistences
func (r *TopicResolver) ProcessorPersistences(ctx context.Context, topic *model.Topic) ([]*model.Processor, error) {
	return r.loaderFunc(ctx).TopicLoader.ProcessorPersistencesByTopic.Load(topic.ID)
}

// Sinks returns the topic's sinks
func (r *TopicResolver) Sinks(ctx context.Context, topic *model.Topic) ([]*model.Sink, error) {
	return r.loaderFunc(ctx).TopicLoader.SinksByTopic.Load(topic.ID)
}

// Sources returns the topic's sources
func (r *TopicResolver) Sources(ctx context.Context, topic *model.Topic) ([]*model.Source, error) {
	return r.loaderFunc(ctx).TopicLoader.SourcesByTopic.Load(topic.ID)
}

// ViewSinks returns the topic's view sinks
func (r *TopicResolver) ViewSinks(ctx context.Context, topic *model.Topic) ([]*model.ViewSink, error) {
	return r.loaderFunc(ctx).TopicLoader.ViewSinksByTopic.Load(topic.ID)
}

// ViewSources returns the topic's view sources
func (r *TopicResolver) ViewSources(ctx context.Context, topic *model.Topic) ([]*model.ViewSource, error) {
	return r.loaderFunc(ctx).TopicLoader.ViewSourcesByTopic.Load(topic.ID)
}

// Views returns the topic's views
func (r *TopicResolver) Views(ctx context.Context, topic *model.Topic) ([]*model.View, error) {
	return r.loaderFunc(ctx).TopicLoader.ViewsByTopic.Load(topic.ID)
}
