package loaders_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	gomock "github.com/golang/mock/gomock"
	"gotest.tools/assert"
)

func Test_Components_Processors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		ProcessorsByComponents(gomock.Any(), []int{12}).
		Return([][]*model.Processor{
			[]*model.Processor{&model.Processor{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorsByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorsByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorsByComponent(13)
	assert.ErrorContains(t, err, "failed to get processors from repository: boom")
}

func Test_Components_Services(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		ServicesByComponents(gomock.Any(), []int{12}).
		Return([]*model.Service{
			&model.Service{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ServicesByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ServiceByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ServiceByComponent(13)
	assert.ErrorContains(t, err, "failed to get services from repository: boom")
}

func Test_Components_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		SinksByComponents(gomock.Any(), []int{12}).
		Return([][]*model.Sink{
			[]*model.Sink{&model.Sink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SinksByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SinksByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SinksByComponent(13)
	assert.ErrorContains(t, err, "failed to get sinks from repository: boom")
}

func Test_Components_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		SourcesByComponents(gomock.Any(), []int{12}).
		Return([][]*model.Source{
			[]*model.Source{&model.Source{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SourcesByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SourcesByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SourcesByComponent(13)
	assert.ErrorContains(t, err, "failed to get sources from repository: boom")
}

func Test_Components_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		ViewSinksByComponents(gomock.Any(), []int{12}).
		Return([][]*model.ViewSink{
			[]*model.ViewSink{&model.ViewSink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSinksByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSinksByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSinksByComponent(13)
	assert.ErrorContains(t, err, "failed to get view sinks from repository: boom")
}

func Test_Components_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		ViewSourcesByComponents(gomock.Any(), []int{12}).
		Return([][]*model.ViewSource{
			[]*model.ViewSource{&model.ViewSource{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSourcesByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSourcesByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSourcesByComponent(13)
	assert.ErrorContains(t, err, "failed to get view sources from repository: boom")
}

func Test_Components_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockComponentRepository(ctrl)
	repository.EXPECT().
		ViewsByComponents(gomock.Any(), []int{12}).
		Return([][]*model.View{
			[]*model.View{&model.View{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewsByComponents(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewComponentLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewsByComponent(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewsByComponent(13)
	assert.ErrorContains(t, err, "failed to get views from repository: boom")
}
