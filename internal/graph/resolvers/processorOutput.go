package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./processorOutput.go -destination=./processorOutput_mock_test.go -package=resolvers_test

// ProcessorOutputLoader is the dataloader for a processor output
type ProcessorOutputLoader interface {
	ProcessorByOutput(int) (*model.Processor, error)
	TopicByOutput(int) (*model.Topic, error)
}

var _ generated.ProcessorOutputResolver = &ProcessorOutputResolver{}

// ProcessorOutputResolver resolves the output's relationships
type ProcessorOutputResolver struct {
	*Resolver
}

// Processor returns the output's processor
func (r *ProcessorOutputResolver) Processor(ctx context.Context, output *model.ProcessorOutput) (*model.Processor, error) {
	result, err := r.DataLoaders.ProcessorOutputLoader(ctx).ProcessorByOutput(output.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor from loader")
	}
	return result, nil
}

// Topic returns the output's topic
func (r *ProcessorOutputResolver) Topic(ctx context.Context, output *model.ProcessorOutput) (*model.Topic, error) {
	result, err := r.DataLoaders.ProcessorOutputLoader(ctx).TopicByOutput(output.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic from loader")
	}
	return result, nil
}
