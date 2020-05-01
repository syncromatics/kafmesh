package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ProcessorInputLoader is the dataloaders for a processor input
type ProcessorInputLoader interface {
	ProcessorByInput(int) (*model.Processor, error)
	TopicByInput(int) (*model.Topic, error)
}

var _ generated.ProcessorInputResolver = &ProcessorInputResolver{}

// ProcessorInputResolver resolves the processor input's relationships
type ProcessorInputResolver struct {
	*Resolver
}

// Processor returns the input's processor
func (r *ProcessorInputResolver) Processor(ctx context.Context, input *model.ProcessorInput) (*model.Processor, error) {
	return r.DataLoaders.ProcessorInputLoader(ctx).ProcessorByInput(input.ID)
}

// Topic returns the input's topic
func (r *ProcessorInputResolver) Topic(ctx context.Context, input *model.ProcessorInput) (*model.Topic, error) {
	return r.DataLoaders.ProcessorInputLoader(ctx).TopicByInput(input.ID)
}
