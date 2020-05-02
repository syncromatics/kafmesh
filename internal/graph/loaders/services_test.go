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

func Test_Services_Components(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockServiceRepository(ctrl)
	repository.EXPECT().
		ComponentsByServices(gomock.Any(), []int{12}).
		Return([][]*model.Component{
			[]*model.Component{&model.Component{}},
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentsByServices(gomock.Any(), []int{13}).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewServiceLoader(context.Background(), repository, 10*time.Millisecond)

	r, err := loader.ComponentsByService(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)

	_, err = loader.ComponentsByService(13)
	assert.ErrorContains(t, err, "failed to get components from repository: boom")
}
