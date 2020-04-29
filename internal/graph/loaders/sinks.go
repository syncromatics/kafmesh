package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// Sinks contains data loaders for sink relationships
type Sinks struct {
	ComponentBySink *ComponentLoader
	PodsBySink      *PodSliceLoader
	TopicBySink     *TopicLoader
}

func configureSinks(loaders *Loaders) {
	loader := &Sinks{}
	loaders.SinkLoader = loader

	loader.ComponentBySink = &ComponentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				sinks.id,
				components.id,
				components.name,
				components.description
			from
				components
			inner join
				sinks on sinks.component=components.id
			where
				sinks.id = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sink components")}
			}
			defer rows.Close()

			components := map[int]*model.Component{}
			var id int
			for rows.Next() {
				component := &model.Component{}
				err = rows.Scan(&id, &component.ID, &component.Name, &component.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan sink row")}
				}
				components[id] = component
			}

			results := []*model.Component{}
			for _, c := range keys {
				s, ok := components[c]
				if !ok {
					return nil, []error{errors.Errorf("did not find component for sink %d", c)}
				}
				results = append(results, s)
			}

			return results, nil
		},
	}

	loader.PodsBySink = &PodSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				pod_sinks.sink,
				pods.id,
				pods.name
			from
				pods
			inner join
				pod_sinks ON pod_sinks.pod=pods.id
			where
				pod_sinks.sink = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sink pods")}
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

	loader.TopicBySink = &TopicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				sinks.id,
				topics.id,
				topics.name,
				topics.message
			from
				topics
			inner join
				sinks on sinks.topic=topics.id
			where
				sinks.id = ANY ($1)
			`, pq.Array(keys))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for sink topic")}
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
