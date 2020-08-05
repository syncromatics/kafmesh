package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Component_Services(t *testing.T) {
	repo := repos.Component()

	r, err := repo.ServicesByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Service{
		&model.Service{
			ID:          1,
			Name:        "service1",
			Description: "service1 description",
		},
		&model.Service{
			ID:          1,
			Name:        "service1",
			Description: "service1 description",
		},
		&model.Service{
			ID:          2,
			Name:        "service2",
			Description: "service2 description",
		},
		&model.Service{
			ID:          2,
			Name:        "service2",
			Description: "service2 description",
		},
	})
}

func Test_Component_Processors(t *testing.T) {
	repo := repos.Component()

	r, err := repo.ProcessorsByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Processor{
		[]*model.Processor{
			&model.Processor{ID: 1, Name: "processor1", Description: "processor1 description"},
			&model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"},
		},
		[]*model.Processor{
			&model.Processor{ID: 3, Name: "processor3", Description: "processor3 description"},
			&model.Processor{ID: 4, Name: "processor4", Description: "processor4 description"},
		},
		[]*model.Processor{},
		[]*model.Processor{},
	})
}

func Test_Component_Sinks(t *testing.T) {
	repo := repos.Component()

	r, err := repo.SinksByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Sink{
		[]*model.Sink{
			&model.Sink{ID: 1, Name: "sink1", Description: "sink1 description"},
			&model.Sink{ID: 2, Name: "sink2", Description: "sink2 description"},
		},
		[]*model.Sink{
			&model.Sink{ID: 3, Name: "sink3", Description: "sink3 description"},
			&model.Sink{ID: 4, Name: "sink4", Description: "sink4 description"},
		},
		[]*model.Sink{},
		[]*model.Sink{},
	})
}

func Test_Component_Sources(t *testing.T) {
	repo := repos.Component()

	r, err := repo.SourcesByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Source{
		[]*model.Source{
			&model.Source{ID: 1},
			&model.Source{ID: 2},
		},
		[]*model.Source{
			&model.Source{ID: 3},
			&model.Source{ID: 4},
		},
		[]*model.Source{},
		[]*model.Source{},
	})
}

func Test_Component_ViewSinks(t *testing.T) {
	repo := repos.Component()

	r, err := repo.ViewSinksByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSink{
		[]*model.ViewSink{
			&model.ViewSink{ID: 1, Name: "viewSink1", Description: "viewSink1 description"},
			&model.ViewSink{ID: 2, Name: "viewSink2", Description: "viewSink2 description"},
		},
		[]*model.ViewSink{
			&model.ViewSink{ID: 3, Name: "viewSink3", Description: "viewSink3 description"},
			&model.ViewSink{ID: 4, Name: "viewSink4", Description: "viewSink4 description"},
		},
		[]*model.ViewSink{},
		[]*model.ViewSink{},
	})
}

func Test_Component_ViewSources(t *testing.T) {
	repo := repos.Component()

	r, err := repo.ViewSourcesByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ViewSource{
		[]*model.ViewSource{
			&model.ViewSource{ID: 1, Description: "viewSource1 description", Name: "viewSource1"},
			&model.ViewSource{ID: 2, Description: "viewSource2 description", Name: "viewSource2"},
		},
		[]*model.ViewSource{
			&model.ViewSource{ID: 3, Description: "viewSource3 description", Name: "viewSource3"},
			&model.ViewSource{ID: 4, Description: "viewSource4 description", Name: "viewSource4"},
		},
		[]*model.ViewSource{},
		[]*model.ViewSource{},
	})
}

func Test_Component_Views(t *testing.T) {
	repo := repos.Component()

	r, err := repo.ViewsByComponents(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.View{
		[]*model.View{
			&model.View{ID: 1},
			&model.View{ID: 2},
		},
		[]*model.View{
			&model.View{ID: 3},
			&model.View{ID: 4},
		},
		[]*model.View{
			&model.View{ID: 5},
		},
		[]*model.View{},
	})
}

func Test_Component_DependsOn(t *testing.T) {
	repo := repos.Component()

	r, err := repo.DependsOn(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Component{
		[]*model.Component{
			&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
		},
		[]*model.Component{
			&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
		},
		[]*model.Component{
			&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
			&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
		},
		[]*model.Component{},
	})
}
