package resolvers_test

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
	"gotest.tools/assert"
)

func Test_Topic_ProcessorInputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorInputsByTopic(12).
		Return([]*model.ProcessorInput{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorInputsByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ProcessorInputs(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ProcessorInputs(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor inputs from loader: boom")
}

func Test_Topic_ProcessorJoins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorJoinsByTopic(12).
		Return([]*model.ProcessorJoin{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorJoinsByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ProcessorJoins(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ProcessorJoins(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor joins from loader: boom")
}

func Test_Topic_ProcessorLookups(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorLookupsByTopic(12).
		Return([]*model.ProcessorLookup{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorLookupsByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ProcessorLookups(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ProcessorLookups(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor lookups from loader: boom")
}

func Test_Topic_ProcessorOutputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorOutputsByTopic(12).
		Return([]*model.ProcessorOutput{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorOutputsByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ProcessorOutputs(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ProcessorOutputs(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor outputs from loader: boom")
}

func Test_Topic_ProcessorPersistences(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ProcessorPersistencesByTopic(12).
		Return([]*model.Processor{}, nil).
		Times(1)

	loader.EXPECT().
		ProcessorPersistencesByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ProcessorPersistences(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ProcessorPersistences(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get processor persistences from loader: boom")
}

func Test_Topic_Sinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SinksByTopic(12).
		Return([]*model.Sink{}, nil).
		Times(1)

	loader.EXPECT().
		SinksByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sinks(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sinks(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get sinks from loader: boom")
}

func Test_Topic_Sources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		SourcesByTopic(12).
		Return([]*model.Source{}, nil).
		Times(1)

	loader.EXPECT().
		SourcesByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Sources(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Sources(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get sources from loader: boom")
}

func Test_Topic_ViewSinks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSinksByTopic(12).
		Return([]*model.ViewSink{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSinksByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSinks(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSinks(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sinks from loader: boom")
}

func Test_Topic_ViewSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewSourcesByTopic(12).
		Return([]*model.ViewSource{}, nil).
		Times(1)

	loader.EXPECT().
		ViewSourcesByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.ViewSources(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.ViewSources(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get view sources from loader: boom")
}

func Test_Topic_Views(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockTopicLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		TopicLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.TopicResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ViewsByTopic(12).
		Return([]*model.View{}, nil).
		Times(1)

	loader.EXPECT().
		ViewsByTopic(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Views(context.Background(), &model.Topic{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Views(context.Background(), &model.Topic{ID: 13})
	assert.ErrorContains(t, err, "failed to get views from loader: boom")
}
