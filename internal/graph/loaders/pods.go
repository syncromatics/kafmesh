package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// Pods contains data loaders for pod relationships
type Pods struct {
	ProcessorsByPod  *ProcessorSliceLoader
	SinksByPod       *SinkSliceLoader
	SourcesByPod     *SourceSliceLoader
	ViewSinksByPod   *ViewSinkSliceLoader
	ViewSourcesByPod *ViewSourceSliceLoader
	ViewsByPod       *ViewSliceLoader
}

func configurePods(loaders *Loaders) {
	loader := &Pods{}
	loaders.PodLoader = loader

	loader.ProcessorsByPod = &ProcessorSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Processor, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_processors.pod,
				processors.id,
				processors.name,
				processors.description
			from
				processors
			inner join
				pod_processors on pod_processors.processor=processors.id
			where
				pod_processors.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processors")}
			}
			defer rows.Close()

			processors := map[int][]*model.Processor{}
			var id int
			for rows.Next() {
				processor := &model.Processor{}
				err = rows.Scan(&id, &processor.ID, &processor.Name, &processor.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor")}
				}
				_, ok := processors[id]
				if !ok {
					processors[id] = []*model.Processor{}
				}

				processors[id] = append(processors[id], processor)
			}

			results := [][]*model.Processor{}
			for _, s := range keys {
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

	loader.SinksByPod = &SinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Sink, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_sinks.pod,
				sinks.id,
				sinks.name,
				sinks.description
			from
				sinks
			inner join
				pod_sinks on pod_sinks.sink=sinks.id
			where
				pod_sinks.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sinks")}
			}
			defer rows.Close()

			sinks := map[int][]*model.Sink{}
			var id int
			for rows.Next() {
				sink := &model.Sink{}
				err = rows.Scan(&id, &sink.ID, &sink.Name, &sink.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan sink")}
				}
				_, ok := sinks[id]
				if !ok {
					sinks[id] = []*model.Sink{}
				}

				sinks[id] = append(sinks[id], sink)
			}

			results := [][]*model.Sink{}
			for _, s := range keys {
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

	loader.SourcesByPod = &SourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Source, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_sources.pod,
				sources.id
			from
				sources
			inner join
				pod_sources on pod_sources.source=sources.id
			where
				pod_sources.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sources")}
			}
			defer rows.Close()

			sources := map[int][]*model.Source{}
			var id int
			for rows.Next() {
				source := &model.Source{}
				err = rows.Scan(&id, &source.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan source")}
				}
				_, ok := sources[id]
				if !ok {
					sources[id] = []*model.Source{}
				}

				sources[id] = append(sources[id], source)
			}

			results := [][]*model.Source{}
			for _, s := range keys {
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

	loader.ViewSinksByPod = &ViewSinkSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSink, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_view_sinks.pod,
				view_sinks.id
			from
				view_sinks
			inner join
				pod_view_sinks on pod_view_sinks.view_sink=view_sinks.id
			where
				pod_view_sinks.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view sinks")}
			}
			defer rows.Close()

			viewSinks := map[int][]*model.ViewSink{}
			var id int
			for rows.Next() {
				viewSink := &model.ViewSink{}
				err = rows.Scan(&id, &viewSink.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view sink")}
				}
				_, ok := viewSinks[id]
				if !ok {
					viewSinks[id] = []*model.ViewSink{}
				}

				viewSinks[id] = append(viewSinks[id], viewSink)
			}

			results := [][]*model.ViewSink{}
			for _, s := range keys {
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

	loader.ViewSourcesByPod = &ViewSourceSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ViewSource, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_view_sources.pod,
				view_sources.id
			from
				view_sources
			inner join
				pod_view_sources on pod_view_sources.view_source=view_sources.id
			where
				pod_view_sources.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view source")}
			}
			defer rows.Close()

			viewSources := map[int][]*model.ViewSource{}
			var id int
			for rows.Next() {
				viewSource := &model.ViewSource{}
				err = rows.Scan(&id, &viewSource.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view source")}
				}
				_, ok := viewSources[id]
				if !ok {
					viewSources[id] = []*model.ViewSource{}
				}

				viewSources[id] = append(viewSources[id], viewSource)
			}

			results := [][]*model.ViewSource{}
			for _, s := range keys {
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

	loader.ViewsByPod = &ViewSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.View, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_views.pod,
				views.id
			from
				views
			inner join
				pod_views on pod_views.view=views.id
			where
				pod_views.pod = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for views")}
			}
			defer rows.Close()

			views := map[int][]*model.View{}
			var id int
			for rows.Next() {
				view := &model.View{}
				err = rows.Scan(&id, &view.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan view")}
				}
				_, ok := views[id]
				if !ok {
					views[id] = []*model.View{}
				}

				views[id] = append(views[id], view)
			}

			results := [][]*model.View{}
			for _, s := range keys {
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
