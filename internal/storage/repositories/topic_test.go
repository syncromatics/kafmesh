package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Topic_ProcessorInputs(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ProcessorInputsByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorInput{
		[]*model.ProcessorInput{
			&model.ProcessorInput{ID: 1},
			&model.ProcessorInput{ID: 3},
		},
		[]*model.ProcessorInput{
			&model.ProcessorInput{ID: 2},
			&model.ProcessorInput{ID: 4},
		},
		[]*model.ProcessorInput{},
		[]*model.ProcessorInput{},
	})
}

func Test_Topic_ProcessorJoins(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ProcessorJoinsByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorJoin{
		[]*model.ProcessorJoin{
			&model.ProcessorJoin{ID: 1},
			&model.ProcessorJoin{ID: 3},
		},
		[]*model.ProcessorJoin{
			&model.ProcessorJoin{ID: 2},
			&model.ProcessorJoin{ID: 4},
		},
		[]*model.ProcessorJoin{},
		[]*model.ProcessorJoin{},
	})
}

func Test_Topic_ProcessorLookups(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ProcessorLookupsByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorLookup{
		[]*model.ProcessorLookup{
			&model.ProcessorLookup{ID: 1},
			&model.ProcessorLookup{ID: 3},
		},
		[]*model.ProcessorLookup{
			&model.ProcessorLookup{ID: 2},
			&model.ProcessorLookup{ID: 4},
		},
		[]*model.ProcessorLookup{},
		[]*model.ProcessorLookup{},
	})
}

func Test_Topic_ProcessorOutputs(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ProcessorOutputsByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorOutput{
		[]*model.ProcessorOutput{
			&model.ProcessorOutput{ID: 1},
			&model.ProcessorOutput{ID: 3},
		},
		[]*model.ProcessorOutput{
			&model.ProcessorOutput{ID: 2},
			&model.ProcessorOutput{ID: 4},
		},
		[]*model.ProcessorOutput{},
		[]*model.ProcessorOutput{},
	})
}

func Test_Topic_ProcessorPersistences(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ProcessorPersistencesByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Processor{
		[]*model.Processor{
			&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description", GroupName: "processor2.group"},
		},
		[]*model.Processor{
			&model.Processor{ID: 3, Name: "processor3", Description: "processor3 description", GroupName: "processor3.group"},
			&model.Processor{ID: 4, Name: "processor4", Description: "processor4 description", GroupName: "processor4.group"},
		},
		[]*model.Processor{},
		[]*model.Processor{},
	})
}

func Test_Topic_Sinks(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.SinksByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Sink{
		[]*model.Sink{
			&model.Sink{ID: 1, Name: "sink1", Description: "sink1 description"},
			&model.Sink{ID: 3, Name: "sink3", Description: "sink3 description"},
		},
		[]*model.Sink{
			&model.Sink{ID: 2, Name: "sink2", Description: "sink2 description"},
			&model.Sink{ID: 4, Name: "sink4", Description: "sink4 description"},
		},
		[]*model.Sink{},
		[]*model.Sink{},
	})
}

func Test_Topic_Sources(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.SourcesByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Source{
		[]*model.Source{
			&model.Source{ID: 1},
			&model.Source{ID: 3},
		},
		[]*model.Source{
			&model.Source{ID: 2},
			&model.Source{ID: 4},
		},
		[]*model.Source{},
		[]*model.Source{},
	})
}

func Test_Topic_ViewSinks(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ViewSinksByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSink{
		[]*model.ViewSink{
			&model.ViewSink{ID: 1, Name: "viewSink1", Description: "viewSink1 description"},
			&model.ViewSink{ID: 3, Name: "viewSink3", Description: "viewSink3 description"},
		},
		[]*model.ViewSink{
			&model.ViewSink{ID: 2, Name: "viewSink2", Description: "viewSink2 description"},
			&model.ViewSink{ID: 4, Name: "viewSink4", Description: "viewSink4 description"},
		},
		[]*model.ViewSink{},
		[]*model.ViewSink{},
	})
}

func Test_Topic_ViewSources(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ViewSourcesByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSource{
		[]*model.ViewSource{
			&model.ViewSource{ID: 1, Name: "viewSource1", Description: "viewSource1 description"},
			&model.ViewSource{ID: 3, Name: "viewSource3", Description: "viewSource3 description"},
		},
		[]*model.ViewSource{
			&model.ViewSource{ID: 2, Name: "viewSource2", Description: "viewSource2 description"},
			&model.ViewSource{ID: 4, Name: "viewSource4", Description: "viewSource4 description"},
		},
		[]*model.ViewSource{},
		[]*model.ViewSource{},
	})
}

func Test_Topic_Views(t *testing.T) {
	repo := repos.Topic()

	r, err := repo.ViewsByTopics(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.View{
		[]*model.View{
			&model.View{ID: 1},
			&model.View{ID: 3},
		},
		[]*model.View{
			&model.View{ID: 2},
			&model.View{ID: 4},
		},
		[]*model.View{},
		[]*model.View{},
	})
}
