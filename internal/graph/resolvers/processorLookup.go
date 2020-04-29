package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ProcessorLookupResolver = &ProcessorLookupResolver{}

// ProcessorLookupResolver resolves the lookup's relationships
type ProcessorLookupResolver struct {
	loader loaderFunc
}

// Processor returns the lookup's processor
func (r *ProcessorLookupResolver) Processor(ctx context.Context, lookup *model.ProcessorLookup) (*model.Processor, error) {
	return r.loader(ctx).ProcessorLookupLoader.ProcessorByLookup.Load(lookup.ID)
}

// Topic returns the lookup's topic
func (r *ProcessorLookupResolver) Topic(ctx context.Context, lookup *model.ProcessorLookup) (*model.Topic, error) {
	return r.loader(ctx).ProcessorLookupLoader.TopicByLookup.Load(lookup.ID)
}
