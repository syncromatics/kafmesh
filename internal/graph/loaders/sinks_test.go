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

func Test_Sinks_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSinkRepository(ctrl)
	repository.EXPECT().
		ComponentBySinks(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentBySinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentBySink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentBySink(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_Sinks_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSinkRepository(ctrl)
	repository.EXPECT().
		PodsBySinks(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsBySinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsBySink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsBySink(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_Sinks_Topict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSinkRepository(ctrl)
	repository.EXPECT().
		TopicBySinks(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicBySinks(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSinkLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicBySink(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicBySink(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
