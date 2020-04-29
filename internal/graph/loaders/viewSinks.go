package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ViewSinks contains data loaders for view sink relationships
type ViewSinks struct {
	ComponentByViewSink *ComponentLoader
	PodsByViewSink      *PodSliceLoader
	TopicByViewSink     *TopicLoader
}

func configureViewSinks(loaders *Loaders) {
	loader := &ViewSinks{}
	loaders.ViewSinkLoader = loader

	loader.ComponentByViewSink = &ComponentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				view_sinks.id,
				components.id,
				components.name,
				components.description
			from
				components
			inner join
				view_sinks on view_sinks.component=components.id
			where
				view_sinks.id = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for components")}
			}
			defer rows.Close()

			components := map[int]*model.Component{}
			var id int
			for rows.Next() {
				component := &model.Component{}
				err = rows.Scan(&id, &component.ID, &component.Name, &component.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan component row")}
				}
				components[id] = component
			}

			results := []*model.Component{}
			for _, c := range keys {
				s, ok := components[c]
				if !ok {
					return nil, []error{errors.Errorf("did not find component for view sink %d", c)}
				}
				results = append(results, s)
			}

			return results, nil
		},
	}

	loader.PodsByViewSink = &PodSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_view_sinks.view_sink,
				pods.id,
				pods.name
			from
				pods
			inner join
				pod_view_sinks ON pod_view_sinks.pod=pods.id
			where
				pod_view_sinks.view_sink = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view sink pods")}
			}
			defer rows.Close()

			pods := map[int][]*model.Pod{}
			var id int
			for rows.Next() {
				pod := &model.Pod{}
				err = rows.Scan(&id, &pod.ID, &pod.Name)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan pods")}
				}
				_, ok := pods[id]
				if !ok {
					pods[id] = []*model.Pod{}
				}

				pods[id] = append(pods[id], pod)
			}

			results := [][]*model.Pod{}
			for _, s := range keys {
				_, ok := pods[s]
				if !ok {
					results = append(results, []*model.Pod{})
				} else {
					results = append(results, pods[s])
				}
			}
			return results, nil
		},
	}

	loader.TopicByViewSink = &TopicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				view_sinks.id,
				topics.id,
				topics.name,
				topics.message
			from
				topics
			inner join
				view_sinks on view_sinks.topic=topics.id
			where
				view_sinks.id = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for view sink topic")}
			}
			defer rows.Close()

			topics := map[int]*model.Topic{}
			var id int
			for rows.Next() {
				topic := &model.Topic{}
				err = rows.Scan(&id, &topic.ID, &topic.Name, &topic.Message)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan topic row")}
				}
				topics[id] = topic
			}

			results := []*model.Topic{}
			for _, c := range keys {
				s, _ := topics[c]
				results = append(results, s)
			}

			return results, nil
		},
	}
}
