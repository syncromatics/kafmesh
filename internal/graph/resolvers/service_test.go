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

func Test_Service_Components(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	loader := NewMockServiceLoader(ctrl)
	loaders := NewMockDataLoaders(ctrl)
	loaders.EXPECT().
		ServiceLoader(gomock.Any()).
		Return(loader).
		Times(2)

	resolver := &resolvers.ServiceResolver{
		Resolver: &resolvers.Resolver{
			DataLoaders: loaders,
		},
	}

	loader.EXPECT().
		ComponentsByService(12).
		Return([]*model.Component{}, nil).
		Times(1)

	loader.EXPECT().
		ComponentsByService(13).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	r, err := resolver.Components(context.Background(), &model.Service{ID: 12})
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = resolver.Components(context.Background(), &model.Service{ID: 13})
	assert.ErrorContains(t, err, "failed to get components from loader: boom")
}
