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

func Test_ViewSource_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSourceRepository(ctrl)
	repository.EXPECT().
		ComponentByViewSources(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentByViewSources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentByViewSource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentByViewSource(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_ViewSource_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSourceRepository(ctrl)
	repository.EXPECT().
		PodsByViewSources(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsByViewSources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsByViewSource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsByViewSource(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_ViewSource_Topic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockViewSourceRepository(ctrl)
	repository.EXPECT().
		TopicByViewSources(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		TopicByViewSources(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewViewSourceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.TopicByViewSource(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.TopicByViewSource(13)
	assert.ErrorContains(t, err, "failed to get topic from repository: boom")
}
