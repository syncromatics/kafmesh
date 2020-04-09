package scraper

import (
	"context"
	"fmt"
	"time"

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

// Scrape scrapes the kafmesh pods running in the kubernetes cluster
func (j *Job) Scrape(ctx context.Context) (map[string]*discoveryv1.Service, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	pods, err := j.getPods(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods")
	}

	results := map[string]*discoveryv1.Service{}

	for _, pod := range pods {
		r, err := j.scrapePod(ctx, pod)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to scrape pod '%s'", pod.Name)
		}
		results[pod.Name] = r
	}

	return results, nil
}

func (j *Job) getPods(ctx context.Context) ([]v1.Pod, error) {
	allPods, err := j.podLister.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list pods")
	}

	scrapablePods := []v1.Pod{}

	for _, pod := range allPods.Items {
		scrapable, ok := pod.Annotations[scrapeAnnotation]
		if ok && scrapable == "true" && pod.Status.Phase == v1.PodRunning {
			scrapablePods = append(scrapablePods, pod)
		}
	}

	return scrapablePods, nil
}

func (j *Job) scrapePod(ctx context.Context, pod v1.Pod) (*discoveryv1.Service, error) {
	port, ok := pod.Annotations[portAnnotation]
	if !ok {
		port = "443"
	}

	url := fmt.Sprintf("%s:%s", pod.Status.PodIP, port)
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
