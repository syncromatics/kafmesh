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

func Test_Views_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewRepository(ctrl)
	repository.EXPECT().
		ComponentByViews(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentByViews(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentByView(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentByView(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_Views_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewRepository(ctrl)
	repository.EXPECT().
		PodsByViews(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsByViews(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsByView(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsByView(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_Views_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewRepository(ctrl)
	repository.EXPECT().
		TopicByViews(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByViews(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByView(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByView(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
