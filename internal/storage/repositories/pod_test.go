package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Pod_Processors(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.ProcessorsByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Processor{
		[]*model.Processor{
			&model.Processor{ID: 1, Name: "processor1", Description: "processor1 description"},
			&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"},
		},
		[]*model.Processor{
			&model.Processor{ID: 1, Name: "processor1", Description: "processor1 description"},
			&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"},
		},
		[]*model.Processor{},
		[]*model.Processor{},
	})
}

func Test_Pod_Sinks(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.SinksByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Sink{
		[]*model.Sink{
			&model.Sink{ID: 1, Name: "sink1", Description: "sink1 description"},
			&model.Sink{ID: 2, Name: "sink2", Description: "sink2 description"},
		},
		[]*model.Sink{
			&model.Sink{ID: 1, Name: "sink1", Description: "sink1 description"},
			&model.Sink{ID: 2, Name: "sink2", Description: "sink2 description"},
		},
		[]*model.Sink{},
		[]*model.Sink{},
	})
}

func Test_Pod_Sources(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.SourcesByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Source{
		[]*model.Source{
			&model.Source{ID: 1},
			&model.Source{ID: 2},
		},
		[]*model.Source{
			&model.Source{ID: 1},
			&model.Source{ID: 2},
		},
		[]*model.Source{},
		[]*model.Source{},
	})
}

func Test_Pod_ViewSinks(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.ViewSinksByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSink{
		[]*model.ViewSink{
			&model.ViewSink{ID: 1},
			&model.ViewSink{ID: 2},
		},
		[]*model.ViewSink{
			&model.ViewSink{ID: 1},
			&model.ViewSink{ID: 2},
		},
		[]*model.ViewSink{},
		[]*model.ViewSink{},
	})
}

func Test_Pod_ViewSources(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.ViewSourcesByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSource{
		[]*model.ViewSource{
			&model.ViewSource{ID: 1},
			&model.ViewSource{ID: 2},
		},
		[]*model.ViewSource{
			&model.ViewSource{ID: 1},
			&model.ViewSource{ID: 2},
		},
		[]*model.ViewSource{},
		[]*model.ViewSource{},
	})
}

func Test_Pod_Views(t *testing.T) {
	repo := repos.Pod()

	r, err := repo.ViewsByPods(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.View{
		[]*model.View{
			&model.View{ID: 1},
			&model.View{ID: 2},
		},
		[]*model.View{
			&model.View{ID: 1},
			&model.View{ID: 2},
		},
		[]*model.View{},
		[]*model.View{},
	})
}
