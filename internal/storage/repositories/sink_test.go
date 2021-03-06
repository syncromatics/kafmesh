package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Sink_Component(t *testing.T) {
	repo := repos.Sink()

	r, err := repo.ComponentBySinks(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Component{
		&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
		&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
		&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
		&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
	})
}

func Test_Sink_Pods(t *testing.T) {
	repo := repos.Sink()

	r, err := repo.PodsBySinks(context.Background(), []int{1, 2, 3, 4})
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

func Test_Sink_Topic(t *testing.T) {
	repo := repos.Sink()

	r, err := repo.TopicBySinks(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Topic{
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
	})
}
