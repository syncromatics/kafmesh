package loaders_test

import (
	"context"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"gotest.tools/assert"
)

func Test_ProcessorJoins_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorJoinRepository(ctrl)
	repository.EXPECT().
		ProcessorByJoins(gomock.Any(), []int{12}).
		Return([]*model.Processor{
			&model.Processor{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorByJoins(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorJoinLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorByJoin(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorByJoin(13)
	assert.ErrorContains(t, err, "failed to get processor from repository: boom")
}

func Test_ProcessorJoins_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorJoinRepository(ctrl)
	repository.EXPECT().
		TopicByJoins(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByJoins(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorJoinLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByJoin(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByJoin(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
