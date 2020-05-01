package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ProcessorLookupLoader is the dataloader for a processor lookup
type ProcessorLookupLoader interface {
	ProcessorByLookup(int) (*model.Processor, error)
	TopicByLookup(int) (*model.Topic, error)
}

var _ generated.ProcessorLookupResolver = &ProcessorLookupResolver{}

// ProcessorLookupResolver resolves the lookup's relationships
type ProcessorLookupResolver struct {
	*Resolver
}

// Processor returns the lookup's processor
func (r *ProcessorLookupResolver) Processor(ctx context.Context, lookup *model.ProcessorLookup) (*model.Processor, error) {
	return r.DataLoaders.ProcessorLookupLoader(ctx).ProcessorByLookup(lookup.ID)
}

// Topic returns the lookup's topic
func (r *ProcessorLookupResolver) Topic(ctx context.Context, lookup *model.ProcessorLookup) (*model.Topic, error) {
	return r.DataLoaders.ProcessorLookupLoader(ctx).TopicByLookup(lookup.ID)
}
