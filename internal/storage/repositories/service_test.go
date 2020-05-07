package repositories_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"gotest.tools/assert"
)

func Test_Service_Components(t *testing.T) {
	repo := repos.Service()

	r, err := repo.ComponentsByServices(context.Background(), []int{1, 2, 3, 4})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, [][]*model.Component{
		[]*model.Component{
			&model.Component{ID: 1, Name: "component1", Description: "component1 description"},
			&model.Component{ID: 2, Name: "component2", Description: "component2 description"},
		},
		[]*model.Component{
			&model.Component{ID: 3, Name: "component3", Description: "component3 description"},
			&model.Component{ID: 4, Name: "component4", Description: "component4 description"},
		},
		[]*model.Component{},
		[]*model.Component{},
	})
}
