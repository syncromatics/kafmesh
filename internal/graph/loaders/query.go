package loaders

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// QueryRepository is the datastore repository for root queries
type QueryRepository interface {
	GetAllServices(context.Context) ([]*model.Service, error)
	GetAllPods(context.Context) ([]*model.Pod, error)
	GetAllTopics(context.Context) ([]*model.Topic, error)
}

var _ resolvers.QueryLoader = &QueryLoader{}

// QueryLoader is the dataloader for root queries
type QueryLoader struct {
	ctx        context.Context
	repository QueryRepository
}

// NewQueryLoader creates a new QueryLoader
func NewQueryLoader(ctx context.Context, repository QueryRepository) *QueryLoader {
	return &QueryLoader{ctx, repository}
}

// GetAllServices returns all services
func (l *QueryLoader) GetAllServices() ([]*model.Service, error) {
	return l.repository.GetAllServices(l.ctx)
}

// GetAllPods returns all pods
func (l *QueryLoader) GetAllPods() ([]*model.Pod, error) {
	return l.repository.GetAllPods(l.ctx)
}

// GetAllTopics returns all topics
func (l *QueryLoader) GetAllTopics() ([]*model.Topic, error) {
	return l.repository.GetAllTopics(l.ctx)
}
