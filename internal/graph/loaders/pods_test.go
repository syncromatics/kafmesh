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

func Test_Pods_Processors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		ProcessorsByPods(gomock.Any(), []int{12}).
		Return([][]*model.Processor{
			[]*model.Processor{&model.Processor{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorsByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorsByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorsByPod(13)
	assert.ErrorContains(t, err, "failed to get processors from repository: boom")
}

func Test_Pods_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		SinksByPods(gomock.Any(), []int{12}).
		Return([][]*model.Sink{
			[]*model.Sink{&model.Sink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SinksByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SinksByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SinksByPod(13)
	assert.ErrorContains(t, err, "failed to get sinks from repository: boom")
}

func Test_Pods_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		SourcesByPods(gomock.Any(), []int{12}).
		Return([][]*model.Source{
			[]*model.Source{&model.Source{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SourcesByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SourcesByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SourcesByPod(13)
	assert.ErrorContains(t, err, "failed to get sources from repository: boom")
}

func Test_Pods_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		ViewSinksByPods(gomock.Any(), []int{12}).
		Return([][]*model.ViewSink{
			[]*model.ViewSink{&model.ViewSink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSinksByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSinksByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSinksByPod(13)
	assert.ErrorContains(t, err, "failed to get view sinks from repository: boom")
}

func Test_Pods_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		ViewSourcesByPods(gomock.Any(), []int{12}).
		Return([][]*model.ViewSource{
			[]*model.ViewSource{&model.ViewSource{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSourcesByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSourcesByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSourcesByPod(13)
	assert.ErrorContains(t, err, "failed to get view sources from repository: boom")
}

func Test_Pods_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockPodRepository(ctrl)
	repository.EXPECT().
		ViewsByPods(gomock.Any(), []int{12}).
		Return([][]*model.View{
			[]*model.View{&model.View{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewsByPods(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewPodLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewsByPod(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewsByPod(13)
	assert.ErrorContains(t, err, "failed to get views from repository: boom")
}
