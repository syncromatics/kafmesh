package subscription

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/resolvers"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//go:generate mockgen -source=./subscribers.go -destination=./subscribers_mock_test.go -package=subscription_test

// PodLister gets the pods running in the cluster
type PodLister interface {
	List(context.Context, metav1.ListOptions) (*corev1.PodList, error)
}

// Factory returns a watch grpc client
type Factory interface {
	Client(ctx context.Context, url string) (Watcher, error)
}

var _ resolvers.Subscribers = &Subscribers{}

// Subscribers provides real time subscription handlers
type Subscribers struct {
	PodLister           PodLister
	Factory             Factory
	ProcessorRepository ProcessorRepository
}

// NewSubscribers creates new subscribers
func NewSubscribers(podLister PodLister, processorRepository ProcessorRepository) *Subscribers {
	return &Subscribers{
		PodLister:           podLister,
		Factory:             &ClientFactory{},
		ProcessorRepository: processorRepository,
	}
}

// Processor returns the processor subscriber handler
func (s *Subscribers) Processor() resolvers.ProcessorWatcher {
	return &Processor{
		Factory:             s.Factory,
		PodLister:           s.PodLister,
		ProcessorRepository: s.ProcessorRepository,
	}
}
