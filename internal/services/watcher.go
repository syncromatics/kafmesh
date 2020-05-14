package services

import (
	"context"

	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./watcher.go -destination=./watcher_mock_test.go -package=services_test

// Watcher watches parts of the kafmesh system
type Watcher interface {
	WatchProcessor(context.Context, *watchv1.ProcessorRequest, func(*watchv1.ProcessorResponse) error) error
}

var _ watchv1.WatchAPIServer = &WatcherService{}

// WatcherService is the grpc interface to the watcher
type WatcherService struct {
	Watcher Watcher
}

// Processor watches a processor
func (s *WatcherService) Processor(request *watchv1.ProcessorRequest, stream watchv1.WatchAPI_ProcessorServer) error {
	err := s.Watcher.WatchProcessor(stream.Context(), request, stream.Send)
	if err != nil {
		return errors.Wrap(err, "failed to watch processor")
	}
	return nil
}
