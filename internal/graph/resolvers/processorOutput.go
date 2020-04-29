package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ProcessorOutputResolver = &ProcessorOutputResolver{}

// ProcessorOutputResolver resolves the output's relationships
type ProcessorOutputResolver struct {
	loader loaderFunc
}

// Processor returns the output's processor
func (r *ProcessorOutputResolver) Processor(ctx context.Context, output *model.ProcessorOutput) (*model.Processor, error) {
	return r.loader(ctx).ProcessorOutputLoader.ProcessorByOutput.Load(output.ID)
}

// Topic returns the output's topic
func (r *ProcessorOutputResolver) Topic(ctx context.Context, output *model.ProcessorOutput) (*model.Topic, error) {
	return r.loader(ctx).ProcessorOutputLoader.TopicByOutput.Load(output.ID)
}
