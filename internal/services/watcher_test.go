package services_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/metadata"
	"gotest.tools/assert"
)

func Test_Watcher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	request := &watchv1.ProcessorRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &stream{
		context: ctx,
	}

	watcher := NewMockWatcher(ctrl)
	watcher.EXPECT().
		WatchProcessor(gomock.Any(), request, gomock.Any()).
		Times(1)

	service := services.WatcherService{watcher}

	err := service.Processor(&watchv1.ProcessorRequest{}, stream)
	assert.NilError(t, err)
}

func Test_WatcherShouldReturnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	request := &watchv1.ProcessorRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &stream{
		context: ctx,
	}

	watcher := NewMockWatcher(ctrl)
	watcher.EXPECT().
		WatchProcessor(gomock.Any(), request, gomock.Any()).
		Return(errors.Errorf("boom")).
		Times(1)

	service := services.WatcherService{watcher}

	err := service.Processor(&watchv1.ProcessorRequest{}, stream)
	assert.ErrorContains(t, err, "failed to watch processor: boom")
}

type stream struct {
	context context.Context
}

func (s *stream) Context() context.Context {
	return s.context
}

func (s *stream) RecvMsg(m interface{}) error {
	return nil
}

func (s *stream) Send(request *watchv1.ProcessorResponse) error {
	return nil
}

func (s *stream) SendHeader(m metadata.MD) error {
	return nil
}

func (s *stream) SendMsg(m interface{}) error {
	return nil
}

func (s *stream) SetHeader(m metadata.MD) error {
	return nil
}

func (s *stream) SetTrailer(m metadata.MD) {
	return
}
