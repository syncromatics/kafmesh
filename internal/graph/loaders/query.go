package loaders

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./query.go -destination=./query_mock_test.go -package=loaders_test

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
	results, err := l.repository.GetAllServices(l.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed getting all services from repository")
	}
	return results, nil
}

// GetAllPods returns all pods
func (l *QueryLoader) GetAllPods() ([]*model.Pod, error) {
	results, err := l.repository.GetAllPods(l.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed getting all pods from repository")
	}
	return results, nil
}

// GetAllTopics returns all topics
func (l *QueryLoader) GetAllTopics() ([]*model.Topic, error) {
	results, err := l.repository.GetAllTopics(l.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed getting all topics from repository")
	}
	return results, nil
}
