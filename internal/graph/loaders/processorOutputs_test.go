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

func Test_ProcessorOutputs_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorOutputRepository(ctrl)
	repository.EXPECT().
		ProcessorByOutputs(gomock.Any(), []int{12}).
		Return([]*model.Processor{
			&model.Processor{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorByOutputs(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorOutputLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorByOutput(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorByOutput(13)
	assert.ErrorContains(t, err, "failed to get processor from repository: boom")
}

func Test_ProcessorOutputs_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorOutputRepository(ctrl)
	repository.EXPECT().
		TopicByOutputs(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByOutputs(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorOutputLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByOutput(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByOutput(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
