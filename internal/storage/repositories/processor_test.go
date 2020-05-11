package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Processor_Component(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.ComponentByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Component{
		&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
		&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
		&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
		&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
	})
}

func Test_Processor_Inputs(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.InputsByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorInput{
		[]*model.ProcessorInput{
			&model.ProcessorInput{ID: 1},
			&model.ProcessorInput{ID: 2},
		},
		[]*model.ProcessorInput{
			&model.ProcessorInput{ID: 3},
			&model.ProcessorInput{ID: 4},
		},
		[]*model.ProcessorInput{},
		[]*model.ProcessorInput{},
	})
}

func Test_Processor_Joins(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.JoinsByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorJoin{
		[]*model.ProcessorJoin{
			&model.ProcessorJoin{ID: 1},
			&model.ProcessorJoin{ID: 2},
		},
		[]*model.ProcessorJoin{
			&model.ProcessorJoin{ID: 3},
			&model.ProcessorJoin{ID: 4},
		},
		[]*model.ProcessorJoin{},
		[]*model.ProcessorJoin{},
	})
}

func Test_Processor_Lookups(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.LookupsByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorLookup{
		[]*model.ProcessorLookup{
			&model.ProcessorLookup{ID: 1},
			&model.ProcessorLookup{ID: 2},
		},
		[]*model.ProcessorLookup{
			&model.ProcessorLookup{ID: 3},
			&model.ProcessorLookup{ID: 4},
		},
		[]*model.ProcessorLookup{},
		[]*model.ProcessorLookup{},
	})
}

func Test_Processor_Outputs(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.OutputsByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.ProcessorOutput{
		[]*model.ProcessorOutput{
			&model.ProcessorOutput{ID: 1},
			&model.ProcessorOutput{ID: 2},
		},
		[]*model.ProcessorOutput{
			&model.ProcessorOutput{ID: 3},
			&model.ProcessorOutput{ID: 4},
		},
		[]*model.ProcessorOutput{},
		[]*model.ProcessorOutput{},
	})
}

func Test_Processor_Pods(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.PodsByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Pod{
		[]*model.Pod{
			&model.Pod{ID: 1, Name: "pod1"},
			&model.Pod{ID: 2, Name: "pod2"},
		},
		[]*model.Pod{
			&model.Pod{ID: 1, Name: "pod1"},
			&model.Pod{ID: 2, Name: "pod2"},
		},
		[]*model.Pod{},
		[]*model.Pod{},
	})
}

func Test_Processor_Persistence(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.PersistenceByProcessors(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Topic{
		nil,
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
	})
}

func Test_Processor_ByID(t *testing.T) {
	repo := repos.Processor()

	r, err := repo.ByID(context.Background(), 2)
	assert.NilError(t, err)
	assert.DeepEqual(t, r, &model.Processor{ID: 2, Name: "processor2", Description: "processor2 description"})
}
