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

func Test_Processor_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentByProcessor(12).
		Return(&model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Component(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Component(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get component from loader: boom")
}

func Test_Processor_Inputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		InputsByProcessor(12).
		Return([]*model.ProcessorInput{}, nil).
		Times(1)

	loader.EXPECT().
		InputsByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Inputs(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Inputs(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get inputs from loader: boom")
}

func Test_Processor_Joins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		JoinsByProcessor(12).
		Return([]*model.ProcessorJoin{}, nil).
		Times(1)

	loader.EXPECT().
		JoinsByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Joins(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Joins(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get joins from loader: boom")
}

func Test_Processor_Lookups(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		LookupsByProcessor(12).
		Return([]*model.ProcessorLookup{}, nil).
		Times(1)

	loader.EXPECT().
		LookupsByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Lookups(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Lookups(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get lookups from loader: boom")
}

func Test_Processor_Outputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		OutputsByProcessor(12).
		Return([]*model.ProcessorOutput{}, nil).
		Times(1)

	loader.EXPECT().
		OutputsByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Outputs(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Outputs(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get outputs from loader: boom")
}

func Test_Processor_Persistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PersistenceByProcessor(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		PersistenceByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Persistence(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Persistence(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get persistence from loader: boom")
}

func Test_Processor_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		PodsByProcessor(12).
		Return([]*model.Pod{}, nil).
		Times(1)

	loader.EXPECT().
		PodsByProcessor(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Pods(context.Background(), &model.Processor{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Pods(context.Background(), &model.Processor{ID: 13})
	assert.ErrorContains(t, err, "failed to get pods from loader: boom")
}
