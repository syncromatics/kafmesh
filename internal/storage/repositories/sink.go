package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ loaders.SinkRepository = &Sink{}

// Sink is the repository for sinks
type Sink struct {
	db *sql.DB
}

// ComponentBySinks returns the components for sinks
func (r *Sink) ComponentBySinks(ctx context.Context, sinks []int) ([]*model.Component, error) {
	rows, err := r.db.QueryContext(ctx, `
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
	`, pq.Array(sinks))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for sink components")
	}
	defer rows.Close()

	components := map[int]*model.Component{}
	var id int
	for rows.Next() {
		component := &model.Component{}
		err = rows.Scan(&id, &component.ID, &component.Name, &component.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan sink row")
		}
		components[id] = component
	}

	results := []*model.Component{}
	for _, c := range sinks {
		s, ok := components[c]
		if !ok {
			return nil, errors.Errorf("did not find component for sink %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// PodsBySinks returns the pods for sinks
func (r *Sink) PodsBySinks(ctx context.Context, sinks []int) ([][]*model.Pod, error) {
	rows, err := r.db.QueryContext(ctx, `
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
			`, pq.Array(sinks))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for sink pods")
	}
	defer rows.Close()

	pods := map[int][]*model.Pod{}
	var id int
	for rows.Next() {
		pod := &model.Pod{}
		err = rows.Scan(&id, &pod.ID, &pod.Name)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan pods")
		}
		_, ok := pods[id]
		if !ok {
			pods[id] = []*model.Pod{}
		}

		pods[id] = append(pods[id], pod)
	}

	results := [][]*model.Pod{}
	for _, s := range sinks {
		_, ok := pods[s]
		if !ok {
			results = append(results, []*model.Pod{})
		} else {
			results = append(results, pods[s])
		}
	}
	return results, nil
}

// TopicBySinks returns the topics for sinks
func (r *Sink) TopicBySinks(ctx context.Context, sinks []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
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
			`, pq.Array(sinks))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for sink topic")
	}
	defer rows.Close()

	topics := map[int]*model.Topic{}
	var id int
	for rows.Next() {
		topic := &model.Topic{}
		err = rows.Scan(&id, &topic.ID, &topic.Name, &topic.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan topic row")
		}
		topics[id] = topic
	}

	results := []*model.Topic{}
	for _, c := range sinks {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
