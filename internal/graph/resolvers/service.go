package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ServiceLoader is the dataloader for a service
type ServiceLoader interface {
	ComponentsByService(int) ([]*model.Component, error)
}

var _ generated.ServiceResolver = &ServiceResolver{}

// ServiceResolver resolves the service's relationships
type ServiceResolver struct {
	*Resolver
}

// Components returns the service's components
func (s *ServiceResolver) Components(ctx context.Context, service *model.Service) ([]*model.Component, error) {
	return s.DataLoaders.ServiceLoader(ctx).ComponentsByService(service.ID)
}
