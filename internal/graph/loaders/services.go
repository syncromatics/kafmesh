package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// Services contains data loaders for service relationships
type Services struct {
	ComponentsByServiceID *ComponentSliceLoader
}

func configureServices(loaders *Loaders) {
	loader := &Services{}
	loaders.ServiceLoader = loader
	loader.ComponentsByServiceID = &ComponentSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Component, []error) {
			var services []int
			for _, key := range keys {
				services = append(services, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
				return nil, []error{errors.Wrap(err, "failed to query for components")}
			}
			defer rows.Close()

			components := map[int][]*model.Component{}
			var serviceID int
			for rows.Next() {
				component := &model.Component{}
				err = rows.Scan(&serviceID, &component.ID, &component.Name, &component.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan component")}
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
		},
	}
}
