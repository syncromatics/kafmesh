package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

var _ generated.QueryResolver = &QueryResolver{}

// QueryResolver resolves querys
type QueryResolver struct {
	loaderFunc loaderFunc
}

// Services gets all the services
func (r *QueryResolver) Services(ctx context.Context) ([]*model.Service, error) {
	return r.loaderFunc(ctx).GetAllServices()
}

// Pods gets all the pods
func (r *QueryResolver) Pods(ctx context.Context) ([]*model.Pod, error) {
	return r.loaderFunc(ctx).GetAllPods()
}

// Topics gets all the topics
func (r *QueryResolver) Topics(ctx context.Context) ([]*model.Topic, error) {
	return r.loaderFunc(ctx).GetAllTopics()
}
