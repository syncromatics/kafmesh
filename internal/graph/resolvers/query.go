package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./query.go -destination=./query_mock_test.go -package=resolvers_test

// QueryLoader is the loader for queries
type QueryLoader interface {
	GetAllServices() ([]*model.Service, error)
	GetAllPods() ([]*model.Pod, error)
	GetAllTopics() ([]*model.Topic, error)
}

var _ generated.QueryResolver = &QueryResolver{}

// QueryResolver resolves querys
type QueryResolver struct {
	*Resolver
}

// Services gets all the services
func (r *QueryResolver) Services(ctx context.Context) ([]*model.Service, error) {
	result, err := r.DataLoaders.QueryLoader(ctx).GetAllServices()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get services from loader")
	}
	return result, nil
}

// Pods gets all the pods
func (r *QueryResolver) Pods(ctx context.Context) ([]*model.Pod, error) {
	result, err := r.DataLoaders.QueryLoader(ctx).GetAllPods()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from loader")
	}
	return result, nil
}

// Topics gets all the topics
func (r *QueryResolver) Topics(ctx context.Context) ([]*model.Topic, error) {
	result, err := r.DataLoaders.QueryLoader(ctx).GetAllTopics()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topics from loader")
	}
	return result, nil
}
