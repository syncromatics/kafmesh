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

func Test_ViewSink_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSinkRepository(ctrl)
	repository.EXPECT().
		ComponentByViewSinks(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentByViewSinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentByViewSink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentByViewSink(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_ViewSink_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSinkRepository(ctrl)
	repository.EXPECT().
		PodsByViewSinks(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsByViewSinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsByViewSink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsByViewSink(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_ViewSink_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSinkRepository(ctrl)
	repository.EXPECT().
		TopicByViewSinks(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByViewSinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByViewSink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByViewSink(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
