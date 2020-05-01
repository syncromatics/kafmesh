package resolvers_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"gotest.tools/assert"
)

func Test_Query_Services(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllServices().
		Return([]*model.Service{}, nil).
		Times(1)

	r, err := resolver.Services(context.Background())
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_ServicesShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllServices().
		Return(nil, errors.Errorf("boom")).
		Times(1)

	_, err := resolver.Services(context.Background())
	assert.ErrorContains(t, err, "failed to get services from loader: boom")
}

func Test_Query_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllPods().
		Return([]*model.Pod{}, nil).
		Times(1)

	r, err := resolver.Pods(context.Background())
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_PodsShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllPods().
		Return(nil, errors.Errorf("boom")).
		Times(1)

	_, err := resolver.Pods(context.Background())
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}

func Test_Query_Topics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllTopics().
		Return([]*model.Topic{}, nil).
		Times(1)

	r, err := resolver.Topics(context.Background())
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_TopicsShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockQueryLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		QueryLoader(gomock.Any()).
		Return(loader).
		Times(1)

	resolver := &resolvers.QueryResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		GetAllTopics().
		Return(nil, errors.Errorf("boom")).
		Times(1)

	_, err := resolver.Topics(context.Background())
	assert.ErrorContains(t, err, "failed to get topics from loader: boom")
}
