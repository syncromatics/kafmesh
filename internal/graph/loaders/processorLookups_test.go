package loaders_test

import (
	"context"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"gotest.tools/assert"
)

func Test_ProcessorLookups_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorLookupRepository(ctrl)
	repository.EXPECT().
		ProcessorByLookups(gomock.Any(), []int{12}).
		Return([]*model.Processor{
			&model.Processor{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorByLookups(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLookupLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorByLookup(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorByLookup(13)
	assert.ErrorContains(t, err, "failed to get processor from repository: boom")
}

func Test_ProcessorLookups_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorLookupRepository(ctrl)
	repository.EXPECT().
		TopicByLookups(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByLookups(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLookupLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByLookup(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByLookup(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
