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

func Test_Processors_Component(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		ComponentByProcessors(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentByProcessor(13)
	assert.ErrorContains(t, err, "failed to get component from repository: boom")
}

func Test_Processors_Inputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		InputsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorInput{
			[]*model.ProcessorInput{&model.ProcessorInput{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		InputsByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.InputsByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.InputsByProcessor(13)
	assert.ErrorContains(t, err, "failed to get inputs from repository: boom")
}

func Test_Processors_Joins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		JoinsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorJoin{
			[]*model.ProcessorJoin{&model.ProcessorJoin{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		JoinsByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.JoinsByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.JoinsByProcessor(13)
	assert.ErrorContains(t, err, "failed to get joins from repository: boom")
}

func Test_Processors_Lookups(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		LookupsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorLookup{
			[]*model.ProcessorLookup{&model.ProcessorLookup{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		LookupsByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.LookupsByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.LookupsByProcessor(13)
	assert.ErrorContains(t, err, "failed to get lookups from repository: boom")
}

func Test_Processors_Outputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		OutputsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorOutput{
			[]*model.ProcessorOutput{&model.ProcessorOutput{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		OutputsByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.OutputsByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.OutputsByProcessor(13)
	assert.ErrorContains(t, err, "failed to get outputs from repository: boom")
}

func Test_Processors_Pods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		PodsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{&model.Pod{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PodsByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PodsByProcessor(13)
	assert.ErrorContains(t, err, "failed to get pods from repository: boom")
}

func Test_Processors_Persistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockProcessorRepository(ctrl)
	repository.EXPECT().
		PersistenceByProcessors(gomock.Any(), []int{12}).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	repository.EXPECT().
		PersistenceByProcessors(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewProcessorLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.PersistenceByProcessor(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.PersistenceByProcessor(13)
	assert.ErrorContains(t, err, "failed to get persistence from repository: boom")
}
