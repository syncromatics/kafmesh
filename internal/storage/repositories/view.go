package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ViewRepository = &View{}

// View is the repository for views
type View struct {
	db *sql.DB
}

// ComponentByViews returns components for views
func (r *View) ComponentByViews(ctx context.Context, views []int) ([]*model.Component, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		views.id,
		components.id,
		components.name,
		components.description
	from
		components
	inner join
		views on views.component=components.id
	where
		views.id = ANY ($1)
	`, pq.Array(views))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for components")
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
	for _, c := range views {
		s, ok := components[c]
		if !ok {
			return nil, errors.Errorf("did not find component for view %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// PodsByViews returns the pods for views
func (r *View) PodsByViews(ctx context.Context, views []int) ([][]*model.Pod, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		pod_views.view,
		pods.id,
		pods.name
	from
		pods
	inner join
		pod_views ON pod_views.pod=pods.id
	where
		pod_views.view = ANY ($1)
	`, pq.Array(views))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for view pods")
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
	for _, s := range views {
		_, ok := pods[s]
		if !ok {
			results = append(results, []*model.Pod{})
		} else {
			results = append(results, pods[s])
		}
	}
	return results, nil
}

// TopicByViews returns the topics for views
func (r *View) TopicByViews(ctx context.Context, views []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		views.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		views on views.topic=topics.id
	where
		views.id = ANY ($1)
	`, pq.Array(views))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for view topic")
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
	for _, c := range views {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
