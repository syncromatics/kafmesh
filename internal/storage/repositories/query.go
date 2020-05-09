package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

var _ loaders.QueryRepository = &Query{}

// Query is the repository for root queries
type Query struct {
	db *sql.DB
}

// GetAllServices returns all services in the datastore
func (r *Query) GetAllServices(ctx context.Context) ([]*model.Service, error) {
	rows, err := r.db.QueryContext(ctx, `select id, name, description from services`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query services")
	}
	defer rows.Close()

	services := []*model.Service{}
	for rows.Next() {
		service := &model.Service{}
		err = rows.Scan(&service.ID, &service.Name, &service.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan service")
		}
		services = append(services, service)
	}

	return services, nil
}

// GetAllPods returns all pods in the datastore
func (r *Query) GetAllPods(ctx context.Context) ([]*model.Pod, error) {
	rows, err := r.db.QueryContext(ctx, `select id, name from pods`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query pods")
	}
	defer rows.Close()

	pods := []*model.Pod{}
	for rows.Next() {
		pod := &model.Pod{}
		err = rows.Scan(&pod.ID, &pod.Name)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan pod")
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

// GetAllTopics returns all topics in the datastore
func (r *Query) GetAllTopics(ctx context.Context) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `select id, name, message from topics`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query topics")
	}
	defer rows.Close()

	results := []*model.Topic{}
	for rows.Next() {
		topic := &model.Topic{}
		err = rows.Scan(&topic.ID, &topic.Name, &topic.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan topic")
		}
		results = append(results, topic)
	}

	return results, nil
}

// ServiceByID gets a service by id
func (r *Query) ServiceByID(ctx context.Context, id int) (*model.Service, error) {
	row := r.db.QueryRowContext(ctx, `
select
	id,
	name,
	description
from
	services
where
	id=$1`, id)

	service := &model.Service{}
	err := row.Scan(&service.ID, &service.Name, &service.Description)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan for service")
	}

	return service, nil
}

// ComponentByID gets a service by id
func (r *Query) ComponentByID(ctx context.Context, id int) (*model.Component, error) {
	row := r.db.QueryRowContext(ctx, `
select
	id,
	name,
	description
from
	components
where
	id=$1`, id)

	component := &model.Component{}
	err := row.Scan(&component.ID, &component.Name, &component.Description)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan for component")
	}

	return component, nil
}
