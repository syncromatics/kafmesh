package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.ServiceResolver = &ServiceResolver{}

// ServiceResolver resolves the service's relationships
type ServiceResolver struct {
	loader loaderFunc
}

// Components returns the service's components
func (s *ServiceResolver) Components(ctx context.Context, service *model.Service) ([]*model.Component, error) {
	return s.loader(ctx).ServiceLoader.ComponentsByServiceID.Load(service.ID)
}
