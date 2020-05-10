package runner

import (
	"context"
	"sync"
	"time"

	"github.com/syncromatics/kafmesh/internal/observability"
	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	pingv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/ping/v1"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// KafaConfigurator configures the kafka topics require to run the service
type KafaConfigurator func(ctx context.Context, brokers []string) error

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
	Metrics      *Metrics
	watcher      *observability.Watcher

	mtx          sync.Mutex
	configured   bool
	running      bool
	runners      []func(context.Context) func() error
	DiscoverInfo *discoveryv1.Service
}

// NewService creates a new kafmesh service
func NewService(brokers []string, protoRegistry *Registry, grpcServer *grpc.Server) *Service {
	service := &Service{
		brokers:      brokers,
		protoWrapper: NewProtoWrapper(protoRegistry),
		server:       grpcServer,
		DiscoverInfo: &discoveryv1.Service{},
		Metrics:      NewMetrics(),
		watcher:      &observability.Watcher{},
	}

	pingv1.RegisterPingAPIServer(grpcServer, &services.PingAPI{})
	discoveryv1.RegisterDiscoveryAPIServer(grpcServer, &services.DiscoverAPI{DiscoverInfo: service.DiscoverInfo})
	watchv1.RegisterWatchAPIServer(grpcServer, &services.WatcherService{Watcher: service.watcher})

	return service
}

// ConfigureKafka waits for kafka to be ready and configures the topics
// for this service. It will also check if topics it doesn't create exist
// in the correct configuration.
func (s *Service) ConfigureKafka(ctx context.Context, configurator KafaConfigurator) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	err := s.waitForKafkaToBeReady(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to talk to kafka")
	}

	err = configurator(ctx, s.brokers)
	if err != nil {
		return errors.Wrap(err, "failed to configure kafka")
	}

	s.configured = true

	return nil
}

// Run executes the kafmesh services
func (s *Service) Run(ctx context.Context) func() error {
	c, cancel := context.WithCancel(ctx)
	grp, c := errgroup.WithContext(c)

	return func() error {
		s.mtx.Lock()
		if !s.configured {
			return errors.Errorf("ConfigureKafka was never called. Please call ConfigureKafka first")
		}

		if s.running {
			return errors.Errorf("Run can only be called once.")
		}

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

func (s *Service) waitForKafkaToBeReady(ctx context.Context) error {
	var lastErr error
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	for {
		var brokers []*sarama.Broker
		var err error
		client, err := sarama.NewClusterAdmin(s.brokers, config)
		if err != nil {
			lastErr = err
			goto checkContext
		}

		brokers, _, err = client.DescribeCluster()
		if err == nil && len(brokers) > 0 {
			client.Close()
			return nil
		}
		lastErr = err

	checkContext:
		select {
		case <-ctx.Done():
			return errors.Wrap(lastErr, "failed waiting for registry to start")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// ProcessorContext creates a processor context
func (s *Service) ProcessorContext(ctx context.Context, component, processor, key string) *ProcessorContext {
	var operation *watchv1.Operation

	_, ok := s.watcher.WatchCount(component, processor, key)
	if ok {
		operation = &watchv1.Operation{}
	}

	return &ProcessorContext{
		Context:   ctx,
		operation: operation,
		watcher:   s.watcher,
		component: component,
		processor: processor,
		key:       key,
	}
}
