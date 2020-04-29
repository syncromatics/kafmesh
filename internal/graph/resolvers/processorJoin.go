package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ProcessorJoinResolver = &ProcessorJoinResolver{}

// ProcessorJoinResolver resolves the join's relationships
type ProcessorJoinResolver struct {
	loader loaderFunc
}

// Processor returns the join's processor
func (r *ProcessorJoinResolver) Processor(ctx context.Context, join *model.ProcessorJoin) (*model.Processor, error) {
	return r.loader(ctx).ProcessorJoinLoader.ProcessorByJoin.Load(join.ID)
}

// Topic returns the join's topic
func (r *ProcessorJoinResolver) Topic(ctx context.Context, join *model.ProcessorJoin) (*model.Topic, error) {
	return r.loader(ctx).ProcessorJoinLoader.TopicByJoin.Load(join.ID)
}
