package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// Components container dataloaders for component relationships
type Components struct {
	ServiceByComponent     *ServiceLoader
	ProcessorsByComponent  *ProcessorSliceLoader
	SinksByComponent       *SinkSliceLoader
	SourcesByComponent     *SourceSliceLoader
	ViewSinksByComponent   *ViewSinkSliceLoader
	ViewSourcesByComponent *ViewSourceSliceLoader
	ViewsByComponent       *ViewSliceLoader
}

func configureComponentLoaders(loaders *Loaders) {
	loader := &Components{}
	loaders.ComponentLoader = loader

	loader.ServiceByComponent = &ServiceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Service, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				components.id,
				services.id,
				services.name,
				services.description
			from
				services
			inner join
				components on components.service=services.id
			where
				service = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for component services")}
			}
			defer rows.Close()

			componentServices := map[int]*model.Service{}
			var componentID int
			for rows.Next() {
				service := &model.Service{}
				err = rows.Scan(&componentID, &service.ID, &service.Name, &service.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan service row")}
				}
			}

			services := []*model.Service{}
			for _, c := range components {
				s, ok := componentServices[c]
				if !ok {
					return nil, []error{errors.Errorf("did not find service for component %d", c)}
				}
				services = append(services, s)
			}

			return services, nil
		},
	}

	loader.ProcessorsByComponent = &ProcessorSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Processor, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id,
				name,
				description
			from
				processors
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processors")}
			}
			defer rows.Close()

			processors := map[int][]*model.Processor{}
			var componentID int
			for rows.Next() {
				processor := &model.Processor{}
				err = rows.Scan(&componentID, &processor.ID, &processor.Name, &processor.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor")}
				}
				_, ok := processors[componentID]
				if !ok {
					processors[componentID] = []*model.Processor{}
				}

				processors[componentID] = append(processors[componentID], processor)
			}

			results := [][]*model.Processor{}
			for _, s := range components {
				_, ok := processors[s]
				if !ok {
					results = append(results, []*model.Processor{})
				} else {
					results = append(results, processors[s])
				}
			}
			return results, nil
		},
	}

	loader.SinksByComponent = &SinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Sink, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id,
				name,
				description
			from
				sinks
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sinks")}
			}
			defer rows.Close()

			sinks := map[int][]*model.Sink{}
			var componentID int
			for rows.Next() {
				sink := &model.Sink{}
				err = rows.Scan(&componentID, &sink.ID, &sink.Name, &sink.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan sink")}
				}
				_, ok := sinks[componentID]
				if !ok {
					sinks[componentID] = []*model.Sink{}
				}

				sinks[componentID] = append(sinks[componentID], sink)
			}

			results := [][]*model.Sink{}
			for _, s := range components {
				_, ok := sinks[s]
				if !ok {
					results = append(results, []*model.Sink{})
				} else {
					results = append(results, sinks[s])
				}
			}
			return results, nil
		},
	}

	loader.SourcesByComponent = &SourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Source, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id
			from
				sources
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sources")}
			}
			defer rows.Close()

			sources := map[int][]*model.Source{}
			var componentID int
			for rows.Next() {
				source := &model.Source{}
				err = rows.Scan(&componentID, &source.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan source")}
				}
				_, ok := sources[componentID]
				if !ok {
					sources[componentID] = []*model.Source{}
				}

				sources[componentID] = append(sources[componentID], source)
			}

			results := [][]*model.Source{}
			for _, s := range components {
				_, ok := sources[s]
				if !ok {
					results = append(results, []*model.Source{})
				} else {
					results = append(results, sources[s])
				}
			}
			return results, nil
		},
	}

	loader.ViewSinksByComponent = &ViewSinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id,
				name,
				description
			from
				view_sinks
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view sinks")}
			}
			defer rows.Close()

			viewSinks := map[int][]*model.ViewSink{}
			var componentID int
			for rows.Next() {
				viewSink := &model.ViewSink{}
				err = rows.Scan(&componentID, &viewSink.ID, &viewSink.Name, &viewSink.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view sink")}
				}
				_, ok := viewSinks[componentID]
				if !ok {
					viewSinks[componentID] = []*model.ViewSink{}
				}

				viewSinks[componentID] = append(viewSinks[componentID], viewSink)
			}

			results := [][]*model.ViewSink{}
			for _, s := range components {
				_, ok := viewSinks[s]
				if !ok {
					results = append(results, []*model.ViewSink{})
				} else {
					results = append(results, viewSinks[s])
				}
			}
			return results, nil
		},
	}

	loader.ViewSourcesByComponent = &ViewSourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id,
				name,
				description
			from
				view_sources
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view sources")}
			}
			defer rows.Close()

			viewSources := map[int][]*model.ViewSource{}
			var componentID int
			for rows.Next() {
				viewSource := &model.ViewSource{}
				err = rows.Scan(&componentID, &viewSource.ID, &viewSource.Name, &viewSource.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view source")}
				}
				_, ok := viewSources[componentID]
				if !ok {
					viewSources[componentID] = []*model.ViewSource{}
				}

				viewSources[componentID] = append(viewSources[componentID], viewSource)
			}

			results := [][]*model.ViewSource{}
			for _, s := range components {
				_, ok := viewSources[s]
				if !ok {
					results = append(results, []*model.ViewSource{})
				} else {
					results = append(results, viewSources[s])
				}
			}
			return results, nil
		},
	}

	loader.ViewsByComponent = &ViewSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.View, []error) {
			var components []int
			for _, key := range keys {
				components = append(components, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				component,
				id
			from
				views
			where
				component = ANY ($1)
			`, pq.Array(components))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for views")}
			}
			defer rows.Close()

			views := map[int][]*model.View{}
			var componentID int
			for rows.Next() {
				view := &model.View{}
				err = rows.Scan(&componentID, &view.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view")}
				}
				_, ok := views[componentID]
				if !ok {
					views[componentID] = []*model.View{}
				}

				views[componentID] = append(views[componentID], view)
			}

			results := [][]*model.View{}
			for _, s := range components {
				_, ok := views[s]
				if !ok {
					results = append(results, []*model.View{})
				} else {
					results = append(results, views[s])
				}
			}
			return results, nil
		},
	}
}
