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

func Test_Source_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		SourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.SourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentBySource(12).
		Return(&model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentBySource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Component(context.Background(), &model.Source{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Component(context.Background(), &model.Source{ID: 13})
	assert.ErrorContains(t, err, "failed to get component from loader: boom")
}

func Test_Source_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		SourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.SourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PodsBySource(12).
		Return([]*model.Pod{}, nil).
		Times(1)

	loader.EXPECT().
		PodsBySource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Pods(context.Background(), &model.Source{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Pods(context.Background(), &model.Source{ID: 13})
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}

func Test_Source_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		SourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.SourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicBySource(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicBySource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.Source{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.Source{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
