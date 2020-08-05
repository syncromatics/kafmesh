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

func Test_View_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentByView(12).
		Return(&model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentByView(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Component(context.Background(), &model.View{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Component(context.Background(), &model.View{ID: 13})
	assert.ErrorContains(t, err, "failed to get component from loader: boom")
}

func Test_View_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PodsByView(12).
		Return([]*model.Pod{}, nil).
		Times(1)

	loader.EXPECT().
		PodsByView(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Pods(context.Background(), &model.View{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Pods(context.Background(), &model.View{ID: 13})
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}

func Test_View_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockViewLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ViewLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ViewResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicByView(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicByView(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.View{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.View{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
