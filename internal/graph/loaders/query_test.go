package loaders_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"gotest.tools/assert"
)

func Test_Query_GetAllServices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllServices(gomock.Any()).
		Return([]*model.Service{
			&model.Service{},
		}, nil).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	r, err := loader.GetAllServices()
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_GetAllServicesShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllServices(gomock.Any()).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	_, err := loader.GetAllServices()
	assert.ErrorContains(t, err, "failed getting all services from repository: boom")
}

func Test_Query_GetAllPods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllPods(gomock.Any()).
		Return([]*model.Pod{
			&model.Pod{},
		}, nil).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	r, err := loader.GetAllPods()
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_GetAllPodsShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllPods(gomock.Any()).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	_, err := loader.GetAllPods()
	assert.ErrorContains(t, err, "failed getting all pods from repository: boom")
}

func Test_Query_GetAllTopics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllTopics(gomock.Any()).
		Return([]*model.Topic{
			&model.Topic{},
		}, nil).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	r, err := loader.GetAllTopics()
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_GetAllTopicsShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		GetAllTopics(gomock.Any()).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	_, err := loader.GetAllTopics()
	assert.ErrorContains(t, err, "failed getting all topics from repository: boom")
}

func Test_Query_ServiceByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		ServiceByID(gomock.Any(), 12).
		Return(&model.Service{}, nil).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	r, err := loader.ServiceByID(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_ServiceByIDShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		ServiceByID(gomock.Any(), 12).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	_, err := loader.ServiceByID(12)
	assert.ErrorContains(t, err, "failed to get service by id from repository: boom")
}

func Test_Query_ComponentByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		ComponentByID(gomock.Any(), 12).
		Return(&model.Component{}, nil).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	r, err := loader.ComponentByID(12)
	assert.NilError(t, err)
	assert.Assert(t, r != nil)
}

func Test_Query_ComponentByIDShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repository := NewMockQueryRepository(ctrl)
	repository.EXPECT().
		ComponentByID(gomock.Any(), 12).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	loader := loaders.NewQueryLoader(context.Background(), repository)

	_, err := loader.ComponentByID(12)
	assert.ErrorContains(t, err, "failed to get component by id from repository: boom")
}
