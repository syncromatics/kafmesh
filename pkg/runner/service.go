package runner

import (
	"context"
	"sync"

	pingv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/ping/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ServiceOptions are the options passed to services
type ServiceOptions struct {
	Brokers      []string
	ProtoWrapper *ProtoWrapper
}

// Service is the kafmesh service
type Service struct {
	brokers      []string
	protoWrapper *ProtoWrapper
	server       *grpc.Server

	mtx     sync.Mutex
	running bool
	runners []func(context.Context) func() error
}

// NewService creates a new kafmesh service
func NewService(brokers []string, protoRegistry *Registry, grpcServer *grpc.Server) *Service {
	pingv1.RegisterPingAPIServer(grpcServer, &services.PingAPI{})

	return &Service{
		brokers:      brokers,
		protoWrapper: NewProtoWrapper(protoRegistry),
		server:       grpcServer,
	}
}

// Run executes the kafmesh services
func (s *Service) Run(ctx context.Context) func() error {
	c, cancel := context.WithCancel(ctx)
	grp, c := errgroup.WithContext(c)

	return func() error {
		s.mtx.Lock()
		for _, r := range s.runners {
			grp.Go(r(c))
		}
		s.running = true
		s.mtx.Unlock()

		select {
		case <-ctx.Done():
			cancel()
			return nil
		case <-c.Done():
			cancel()
			return grp.Wait()
		}
	}
}

// RegisterRunner registers a runner with the service. Will return error if service is running
func (s *Service) RegisterRunner(runner func(context.Context) func() error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.running {
		return errors.Errorf("failed to register running because service is already running")
	}

	s.runners = append(s.runners, runner)
	return nil
}

// Options returns service options for runners
func (s *Service) Options() ServiceOptions {
	return ServiceOptions{
		Brokers:      s.brokers,
		ProtoWrapper: s.protoWrapper,
	}
}
