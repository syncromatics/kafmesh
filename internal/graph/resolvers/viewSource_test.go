package resolvers_test

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
	"gotest.tools/assert"
)

func Test_ViewSource_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentByViewSource(12).
		Return(&model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentByViewSource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Component(context.Background(), &model.ViewSource{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Component(context.Background(), &model.ViewSource{ID: 13})
	assert.ErrorContains(t, err, "failed to get component from loader: boom")
}

func Test_ViewSource_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PodsByViewSource(12).
		Return([]*model.Pod{}, nil).
		Times(1)

	loader.EXPECT().
		PodsByViewSource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Pods(context.Background(), &model.ViewSource{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Pods(context.Background(), &model.ViewSource{ID: 13})
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}

func Test_ViewSource_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSourceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSourceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSourceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicByViewSource(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicByViewSource(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.ViewSource{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.ViewSource{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
