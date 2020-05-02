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

func Test_Topics_Inputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ProcessorInputsByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorInput{
			[]*model.ProcessorInput{&model.ProcessorInput{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorInputsByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorInputsByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorInputsByTopic(13)
	assert.ErrorContains(t, err, "failed to get inputs from repository: boom")
}

func Test_Topics_Joins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ProcessorJoinsByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorJoin{
			[]*model.ProcessorJoin{&model.ProcessorJoin{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorJoinsByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorJoinsByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorJoinsByTopic(13)
	assert.ErrorContains(t, err, "failed to get joins from repository: boom")
}

func Test_Topics_Lookups(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ProcessorLookupsByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorLookup{
			[]*model.ProcessorLookup{&model.ProcessorLookup{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorLookupsByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorLookupsByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorLookupsByTopic(13)
	assert.ErrorContains(t, err, "failed to get lookups from repository: boom")
}

func Test_Topics_Outputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ProcessorOutputsByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ProcessorOutput{
			[]*model.ProcessorOutput{&model.ProcessorOutput{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorOutputsByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorOutputsByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorOutputsByTopic(13)
	assert.ErrorContains(t, err, "failed to get outputs from repository: boom")
}

func Test_Topics_ProcessorPersistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ProcessorPersistencesByTopics(gomock.Any(), []int{12}).
		Return([][]*model.Processor{
			[]*model.Processor{&model.Processor{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ProcessorPersistencesByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ProcessorPersistencesByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ProcessorPersistencesByTopic(13)
	assert.ErrorContains(t, err, "failed to get processors from repository: boom")
}

func Test_Topics_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		SinksByTopics(gomock.Any(), []int{12}).
		Return([][]*model.Sink{
			[]*model.Sink{&model.Sink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SinksByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SinksByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SinksByTopic(13)
	assert.ErrorContains(t, err, "failed to get sinks from repository: boom")
}

func Test_Topics_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		SourcesByTopics(gomock.Any(), []int{12}).
		Return([][]*model.Source{
			[]*model.Source{&model.Source{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		SourcesByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.SourcesByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.SourcesByTopic(13)
	assert.ErrorContains(t, err, "failed to get sources from repository: boom")
}

func Test_Topics_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ViewSinksByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ViewSink{
			[]*model.ViewSink{&model.ViewSink{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSinksByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSinksByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSinksByTopic(13)
	assert.ErrorContains(t, err, "failed to get view sinks from repository: boom")
}

func Test_Topics_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ViewSourcesByTopics(gomock.Any(), []int{12}).
		Return([][]*model.ViewSource{
			[]*model.ViewSource{&model.ViewSource{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewSourcesByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewSourcesByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewSourcesByTopic(13)
	assert.ErrorContains(t, err, "failed to get view sources from repository: boom")
}

func Test_Topics_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockTopicRepository(ctrl)
	repository.EXPECT().
		ViewsByTopics(gomock.Any(), []int{12}).
		Return([][]*model.View{
			[]*model.View{&model.View{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ViewsByTopics(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewTopicLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ViewsByTopic(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ViewsByTopic(13)
	assert.ErrorContains(t, err, "failed to get views from repository: boom")
}
