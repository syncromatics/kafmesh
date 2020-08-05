package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Query_GetAllServices(t *testing.T) {
	repo := repos.Query()

	r, err := repo.GetAllServices(context.Background())
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Service{
		&model.Service{ID: 1, Name: "service1", Description: "service1 description"},
		&model.Service{ID: 2, Name: "service2", Description: "service2 description"},
		&model.Service{ID: 3, Name: "service3", Description: "service3 description"},
		&model.Service{ID: 4, Name: "service4", Description: "service4 description"},
	})
}

func Test_Query_GetAllPods(t *testing.T) {
	repo := repos.Query()

	r, err := repo.GetAllPods(context.Background())
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Pod{
		&model.Pod{ID: 1, Name: "pod1"},
		&model.Pod{ID: 2, Name: "pod2"},
	})
}

func Test_Query_GetAllTopics(t *testing.T) {
	repo := repos.Query()

	r, err := repo.GetAllTopics(context.Background())
	assert.NilError(t, err)
	assert.DeepEqual(t, r, []*model.Topic{
		&model.Topic{ID: 1, Name: "topic1", Message: "topic1.message"},
		&model.Topic{ID: 2, Name: "topic2", Message: "topic2.message"},
		&model.Topic{ID: 3, Name: "topic3", Message: "topic3.message"},
		&model.Topic{ID: 4, Name: "topic4", Message: "topic4.message"},
	})
}

func Test_Query_ServiceByID(t *testing.T) {
	repo := repos.Query()

	r, err := repo.ServiceByID(context.Background(), 2)
	assert.NilError(t, err)
	assert.DeepEqual(t, r, &model.Service{ID: 2, Name: "service2", Description: "service2 description"})
}

func Test_Query_ComponentByID(t *testing.T) {
	repo := repos.Query()

	r, err := repo.ComponentByID(context.Background(), 2)
	assert.NilError(t, err)
	assert.DeepEqual(t, r, &model.Component{ID: 2, Name: "component2", Description: "component2 description"})
}
