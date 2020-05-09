package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./service.go -destination=./service_mock_test.go -package=resolvers_test

// ServiceLoader is the dataloader for a service
type ServiceLoader interface {
	ComponentsByService(int) ([]*model.Component, error)
	DependsOn(int) ([]*model.Service, error)
}

var _ generated.ServiceResolver = &ServiceResolver{}

// ServiceResolver resolves the service's relationships
type ServiceResolver struct {
	*Resolver
}

// Components returns the service's components
func (s *ServiceResolver) Components(ctx context.Context, service *model.Service) ([]*model.Component, error) {
	results, err := s.DataLoaders.ServiceLoader(ctx).ComponentsByService(service.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get components from loader")
	}
	return results, nil
}

// DependsOn returns the services that produce topics this service depends on
func (s *ServiceResolver) DependsOn(ctx context.Context, service *model.Service) ([]*model.Service, error) {
	results, err := s.DataLoaders.ServiceLoader(ctx).DependsOn(service.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get dependent services from loader")
	}
	return results, nil
}
