package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.SourceRepository = &Source{}

// Source is the repository for sources
type Source struct {
	db *sql.DB
}

// ComponentBySources returns the components for sources
func (r *Source) ComponentBySources(ctx context.Context, sources []int) ([]*model.Component, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		sources.id,
		components.id,
		components.name,
		components.description
	from
		components
	inner join
		sources on sources.component=components.id
	where
		sources.id = ANY ($1)
	`, pq.Array(sources))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for source components")
	}
	defer rows.Close()

	components := map[int]*model.Component{}
	var id int
	for rows.Next() {
		component := &model.Component{}
		err = rows.Scan(&id, &component.ID, &component.Name, &component.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan component row")
		}
		components[id] = component
	}

	results := []*model.Component{}
	for _, c := range sources {
		s, ok := components[c]
		if !ok {
			return nil, errors.Errorf("did not find component for sink %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// PodsBySources returns the pods for sources
func (r *Source) PodsBySources(ctx context.Context, sources []int) ([][]*model.Pod, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		pod_sources.source,
		pods.id,
		pods.name
	from
		pods
	inner join
		pod_sources ON pod_sources.pod=pods.id
	where
		pod_sources.source = ANY ($1)
	`, pq.Array(sources))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for pods")
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
	for _, s := range sources {
		_, ok := pods[s]
		if !ok {
			results = append(results, []*model.Pod{})
		} else {
			results = append(results, pods[s])
		}
	}
	return results, nil
}

// TopicBySources returns the topics for sources
func (r *Source) TopicBySources(ctx context.Context, sources []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		sources.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		sources on sources.topic=topics.id
	where
		sources.id = ANY ($1)
	`, pq.Array(sources))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for source topic")
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
	for _, c := range sources {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
