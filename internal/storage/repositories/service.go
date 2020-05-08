package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ServiceRepository = &Service{}

// Service is the repository for services
type Service struct {
	db *sql.DB
}

// ComponentsByServices returns components by services
func (r *Service) ComponentsByServices(ctx context.Context, services []int) ([][]*model.Component, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		service,
		id,
		name,
		description
	from
		components
	where
		service = ANY ($1)
	`, pq.Array(services))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for components")
	}
	defer rows.Close()

	components := map[int][]*model.Component{}
	var serviceID int
	for rows.Next() {
		component := &model.Component{}
		err = rows.Scan(&serviceID, &component.ID, &component.Name, &component.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan component")
		}
		_, ok := components[serviceID]
		if !ok {
			components[serviceID] = []*model.Component{}
		}

		components[serviceID] = append(components[serviceID], component)
	}

	results := [][]*model.Component{}
	for _, s := range services {
		_, ok := components[s]
		if !ok {
			results = append(results, []*model.Component{})
		} else {
			results = append(results, components[s])
		}
	}
	return results, nil
}

// DependsOn gets services that depend on services
func (r *Service) DependsOn(ctx context.Context, ids []int) ([][]*model.Service, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		service_topic_dependencies.service,
		services.id,
		services.name,
		services.description
	from
		services
	inner join
		service_topic_sources on service_topic_sources.service=services.id
	inner join
		service_topic_dependencies on service_topic_dependencies.topic=service_topic_sources.topic
	where
		service_topic_sources.service != service_topic_dependencies.service and
		service_topic_dependencies.service = ANY ($1)
	`, pq.Array(ids))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for dependent services")
	}
	defer rows.Close()

	services := map[int][]*model.Service{}
	var serviceID int
	for rows.Next() {
		service := &model.Service{}
		err = rows.Scan(&serviceID, &service.ID, &service.Name, &service.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan service")
		}
		_, ok := services[serviceID]
		if !ok {
			services[serviceID] = []*model.Service{}
		}

		services[serviceID] = append(services[serviceID], service)
	}

	results := [][]*model.Service{}
	for _, s := range ids {
		_, ok := services[s]
		if !ok {
			results = append(results, []*model.Service{})
		} else {
			results = append(results, services[s])
		}
	}
	return results, nil
}
