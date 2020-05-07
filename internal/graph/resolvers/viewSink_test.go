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

func Test_ViewSink_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSinkLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSinkLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSinkResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentByViewSink(12).
		Return(&model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentByViewSink(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Component(context.Background(), &model.ViewSink{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Component(context.Background(), &model.ViewSink{ID: 13})
	assert.ErrorContains(t, err, "failed to get component from loader: boom")
}

func Test_ViewSink_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSinkLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSinkLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSinkResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PodsByViewSink(12).
		Return([]*model.Pod{}, nil).
		Times(1)

	loader.EXPECT().
		PodsByViewSink(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Pods(context.Background(), &model.ViewSink{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Pods(context.Background(), &model.ViewSink{ID: 13})
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}

func Test_ViewSink_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewSinkLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewSinkLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewSinkResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicByViewSink(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicByViewSink(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.ViewSink{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.ViewSink{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
