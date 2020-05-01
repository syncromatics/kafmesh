package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorJoin.go -destination=./processorJoin_mock_test.go -package=resolvers_test

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
	result, err := r.DataLoaders.ProcessorJoinLoader(ctx).ProcessorByJoin(join.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor from loader")
	}
	return result, nil
}

// Topic returns the join's topic
func (r *ProcessorJoinResolver) Topic(ctx context.Context, join *model.ProcessorJoin) (*model.Topic, error) {
	result, err := r.DataLoaders.ProcessorJoinLoader(ctx).TopicByJoin(join.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return result, nil
}
