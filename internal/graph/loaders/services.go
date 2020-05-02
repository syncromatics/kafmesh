package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./services.go -destination=./services_mock_test.go -package=loaders_test

// ServiceRepository is the datastore repository for services
type ServiceRepository interface {
	ComponentsByServices(ctx context.Context, services []int) ([][]*model.Component, error)
}

var _ resolvers.ServiceLoader = &ServiceLoader{}

// ServiceLoader contains data loaders for service relationships
type ServiceLoader struct {
	componentsByServiceID *generated.ComponentSliceLoader
}

// NewServiceLoader creates a new ServiceLoader
func NewServiceLoader(ctx context.Context, repository ServiceRepository, waitTime time.Duration) *ServiceLoader {
	loader := &ServiceLoader{}
	loader.componentsByServiceID = generated.NewComponentSliceLoader(generated.ComponentSliceLoaderConfig{
		Wait:     waitTime,
		MaxBatch: 100,
		Fetch: func(keys []int) ([][]*model.Component, []error) {
			r, err := repository.ComponentsByServices(ctx, keys)
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to get components from repository")}
			}

			return r, nil
		},
	})

	return loader
}

// ComponentsByService returns components for the service
func (l *ServiceLoader) ComponentsByService(serviceID int) ([]*model.Component, error) {
	return l.componentsByServiceID.Load(serviceID)
}
