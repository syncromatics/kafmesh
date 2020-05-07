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

func Test_Pod_Processors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorsByPod(12).
		Return([]*model.Processor{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorsByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Processors(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Processors(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get processors from loader: boom")
}

func Test_Pod_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SinksByPod(12).
		Return([]*model.Sink{}, nil).
		Times(1)

	loader.EXPECT().
		SinksByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sinks(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sinks(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get sinks from loader: boom")
}

func Test_Pod_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SourcesByPod(12).
		Return([]*model.Source{}, nil).
		Times(1)

	loader.EXPECT().
		SourcesByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sources(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sources(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get sources from loader: boom")
}

func Test_Pod_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSinksByPod(12).
		Return([]*model.ViewSink{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSinksByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSinks(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSinks(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sinks from loader: boom")
}

func Test_Pod_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSourcesByPod(12).
		Return([]*model.ViewSource{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSourcesByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSources(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSources(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sources from loader: boom")
}

func Test_Pod_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockPodLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		PodLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.PodResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewsByPod(12).
		Return([]*model.View{}, nil).
		Times(1)

	loader.EXPECT().
		ViewsByPod(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Views(context.Background(), &model.Pod{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Views(context.Background(), &model.Pod{ID: 13})
	assert.ErrorContains(t, err, "failed to get views from loader: boom")
}
