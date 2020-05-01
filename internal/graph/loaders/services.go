package loaders

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// ServiceRepository is the datastore repository for services
type ServiceRepository interface {
	ComponentsByServices(ctx context.Context, services []int) ([][]*model.Component, error)
}

var _ resolvers.ServiceLoader = &ServiceLoader{}

// ServiceLoader contains data loaders for service relationships
type ServiceLoader struct {
	componentsByServiceID *componentSliceLoader
}

// NewServiceLoader creates a new ServiceLoader
func NewServiceLoader(ctx context.Context, repository ServiceRepository) *ServiceLoader {
	loader := &ServiceLoader{}
	loader.componentsByServiceID = &componentSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Component, []error) {
			r, err := repository.ComponentsByServices(ctx, keys)
			if err != nil {
				return nil, []error{err}
			}

			return r, nil
		},
	}

	return loader
}

// ComponentsByService returns components for the service
func (l *ServiceLoader) ComponentsByService(serviceID int) ([]*model.Component, error) {
	return l.componentsByServiceID.Load(serviceID)
}
