package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ProcessorJoinLoader is the dataloader for a processor join
type ProcessorJoinLoader interface {
	ProcessorByJoin(int) (*model.Processor, error)
	TopicByJoin(int) (*model.Topic, error)
}

var _ generated.ProcessorJoinResolver = &ProcessorJoinResolver{}

// ProcessorJoinResolver resolves the join's relationships
type ProcessorJoinResolver struct {
	*Resolver
}

// Processor returns the join's processor
func (r *ProcessorJoinResolver) Processor(ctx context.Context, join *model.ProcessorJoin) (*model.Processor, error) {
	return r.DataLoaders.ProcessorJoinLoader(ctx).ProcessorByJoin(join.ID)
}

// Topic returns the join's topic
func (r *ProcessorJoinResolver) Topic(ctx context.Context, join *model.ProcessorJoin) (*model.Topic, error) {
	return r.DataLoaders.ProcessorJoinLoader(ctx).TopicByJoin(join.ID)
}
