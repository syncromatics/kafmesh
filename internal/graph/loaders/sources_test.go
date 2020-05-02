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

func Test_Sources_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSourceRepository(ctrl)
	repository.EXPECT().
		ComponentBySources(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentBySources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentBySource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentBySource(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_Sources_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSourceRepository(ctrl)
	repository.EXPECT().
		PodsBySources(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsBySources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsBySource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsBySource(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_Sources_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockSourceRepository(ctrl)
	repository.EXPECT().
		TopicBySources(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicBySources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicBySource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicBySource(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
