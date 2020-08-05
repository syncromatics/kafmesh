package scraper

import (
	"context"
	"fmt"
	"strconv"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//go:generate mockgen -source=./job.go -destination=./job_mock_test.go -package=scraper_test

const (
	scrapeAnnotation = "kafmesh/scrape"
	portAnnotation   = "kafmesh/port"
)

// PodLister gets the pods running in the cluster
type PodLister interface {
	List(context.Context, metav1.ListOptions) (*v1.PodList, error)
}

// DiscoveryClient is the kafmesh discovery grpc client
type DiscoveryClient interface {
	GetServiceInfo(context.Context, *discoveryv1.GetServiceInfoRequest, ...grpc.CallOption) (*discoveryv1.GetServiceInfoResponse, error)
}

// DiscoveryFactory creates DiscoveryClients for pods
type DiscoveryFactory interface {
	Client(context.Context, string) (DiscoveryClient, func() error, error)
}

// Job runs scrape jobs against pods running in the cluster
type Job struct {
	podLister        PodLister
	discoveryFactory DiscoveryFactory
}

// NewJob creates a new job
func NewJob(podLister PodLister, discoveryFactory DiscoveryFactory) *Job {
	return &Job{podLister, discoveryFactory}
}

// GetKafmeshPods gets all kafmesh pods running in the cluster
func (j *Job) GetKafmeshPods(ctx context.Context) ([]v1.Pod, error) {
	allPods, err := j.podLister.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list pods")
	}

	scrapablePods := []v1.Pod{}

	for _, pod := range allPods.Items {
		scrapable, ok := pod.Annotations[scrapeAnnotation]
		if !ok {
			continue
		}

		if scrapable == "true" {
			scrapablePods = append(scrapablePods, pod)
		}
	}

	return scrapablePods, nil
}

// ScrapePod gets the kafmesh service info from the pod
func (j *Job) ScrapePod(ctx context.Context, pod v1.Pod) (*discoveryv1.Service, error) {
	port, ok := pod.Annotations[portAnnotation]
	if !ok {
		port = "443"
	}

	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, errors.Wrapf(err, "port annotation value '%s' is not a number", port)
	}

	url := fmt.Sprintf("%s:%d", pod.Status.PodIP, portInt)
	discover, closer, err := j.discoveryFactory.Client(ctx, url)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get discovery client for service '%s'", url)
	}
	defer closer()

	response, err := discover.GetServiceInfo(ctx, &discoveryv1.GetServiceInfoRequest{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed getting service info for service '%s'", url)
	}

	return response.Service, nil
}
