package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ProcessorInputResolver = &ProcessorInputResolver{}

// ProcessorInputResolver resolves the processor input's relationships
type ProcessorInputResolver struct {
	loader loaderFunc
}

// Processor returns the input's processor
func (r *ProcessorInputResolver) Processor(ctx context.Context, input *model.ProcessorInput) (*model.Processor, error) {
	return r.loader(ctx).ProcessorInputLoader.ProcessorByInput.Load(input.ID)
}

// Topic returns the input's topic
func (r *ProcessorInputResolver) Topic(ctx context.Context, input *model.ProcessorInput) (*model.Topic, error) {
	return r.loader(ctx).ProcessorInputLoader.TopicByInput.Load(input.ID)
}
