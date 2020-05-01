package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

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
	return r.DataLoaders.QueryLoader(ctx).GetAllServices()
}

// Pods gets all the pods
func (r *QueryResolver) Pods(ctx context.Context) ([]*model.Pod, error) {
	return r.DataLoaders.QueryLoader(ctx).GetAllPods()
}

// Topics gets all the topics
func (r *QueryResolver) Topics(ctx context.Context) ([]*model.Topic, error) {
	return r.DataLoaders.QueryLoader(ctx).GetAllTopics()
}
