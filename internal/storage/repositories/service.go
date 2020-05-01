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
