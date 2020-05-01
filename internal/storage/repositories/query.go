package repositories

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"
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
