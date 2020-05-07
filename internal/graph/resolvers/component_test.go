package resolvers_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"gotest.tools/assert"
)

func Test_Component_Service(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ServiceByComponent(12).
		Return(&model.Service{}, nil).
		Times(1)

	loader.EXPECT().
		ServiceByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Service(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Service(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get service from loader: boom")
}

func Test_Component_Processors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorsByComponent(12).
		Return([]*model.Processor{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorsByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Processors(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Processors(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get processors from loader: boom")
}

func Test_Component_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SinksByComponent(12).
		Return([]*model.Sink{}, nil).
		Times(1)

	loader.EXPECT().
		SinksByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sinks(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sinks(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get sinks from loader: boom")
}

func Test_Component_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SourcesByComponent(12).
		Return([]*model.Source{}, nil).
		Times(1)

	loader.EXPECT().
		SourcesByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sources(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sources(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get sources from loader: boom")
}

func Test_Component_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSinksByComponent(12).
		Return([]*model.ViewSink{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSinksByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSinks(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSinks(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sinks from loader: boom")
}

func Test_Component_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSourcesByComponent(12).
		Return([]*model.ViewSource{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSourcesByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSources(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSources(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sources from loader: boom")
}

func Test_Component_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockComponentLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ComponentLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ComponentResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewsByComponent(12).
		Return([]*model.View{}, nil).
		Times(1)

	loader.EXPECT().
		ViewsByComponent(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Views(context.Background(), &model.Component{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Views(context.Background(), &model.Component{ID: 13})
	assert.ErrorContains(t, err, "failed to get views from loader: boom")
}
