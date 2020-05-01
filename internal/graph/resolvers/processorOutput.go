package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.ProcessorOutputLoader(ctx).ProcessorByOutput(output.ID)
}

// Topic returns the output's topic
func (r *ProcessorOutputResolver) Topic(ctx context.Context, output *model.ProcessorOutput) (*model.Topic, error) {
	return r.DataLoaders.ProcessorOutputLoader(ctx).TopicByOutput(output.ID)
}
