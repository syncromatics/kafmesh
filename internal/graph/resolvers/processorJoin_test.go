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

func Test_ProcessorJoin_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorJoinLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorJoinLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorJoinResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorByJoin(12).
		Return(&model.Processor{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorByJoin(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Processor(context.Background(), &model.ProcessorJoin{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Processor(context.Background(), &model.ProcessorJoin{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor from loader: boom")
}

func Test_ProcessorJoin_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockProcessorJoinLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ProcessorJoinLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ProcessorJoinResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		TopicByJoin(12).
		Return(&model.Topic{}, nil).
		Times(1)

	loader.EXPECT().
		TopicByJoin(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Topic(context.Background(), &model.ProcessorJoin{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Topic(context.Background(), &model.ProcessorJoin{ID: 13})
	assert.ErrorContains(t, err, "failed to get topic from loader: boom")
}
