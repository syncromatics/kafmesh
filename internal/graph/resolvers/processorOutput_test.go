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

func Test_ProcessorOutput_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorOutputLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorOutputLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorOutputResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorByOutput(12).
		Return(&model.Processor{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorByOutput(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Processor(context.Background(), &model.ProcessorOutput{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Processor(context.Background(), &model.ProcessorOutput{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor from loader: boom")
}

func Test_ProcessorOutput_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorOutputLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorOutputLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorOutputResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicByOutput(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicByOutput(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.ProcessorOutput{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.ProcessorOutput{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
