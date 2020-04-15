package scraper_test

import (
	"context"
	"testing"
	"time"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	scraper "github.com/syncromatics/kafmesh/internal/scraper"

	gomock "github.com/golang/mock/gomock"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_Job(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lister := NewMockPodLister(ctrl)
	discoverFactory := NewMockDiscoveryFactory(ctrl)

	lister.EXPECT().
		List(gomock.Any(), gomock.Any()).
		Return(&v1.PodList{
			Items: []v1.Pod{
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod1",
						Annotations: map[string]string{
							"kafmesh/scrape": "true",
						},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						PodIP: "1.1.1.1",
					},
				},
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod2",
						Annotations: map[string]string{
							"kafmesh/scrape": "true",
							"kafmesh/port":   "7777",
						},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						PodIP: "1.1.1.2",
					},
				},
				v1.Pod{},
				v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "pod8",
						Annotations: map[string]string{},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						PodIP: "1.1.1.2",
					},
				},
			},
		}, nil).
		Times(1)

	client1 := NewMockDiscoveryClient(ctrl)

	discoverFactory.EXPECT().
		Client(gomock.Any(), "1.1.1.1:443").
		Return(client1, func() error { return nil }, nil).
		Times(1)

	client1.EXPECT().
		GetServiceInfo(gomock.Any(), &discoveryv1.GetServiceInfoRequest{}).
		Return(&discoveryv1.GetServiceInfoResponse{
			Service: &discoveryv1.Service{
				Name: "service1",
			},
		}, nil).
		Times(1)

	client2 := NewMockDiscoveryClient(ctrl)

	discoverFactory.EXPECT().
		Client(gomock.Any(), "1.1.1.2:7777").
		Return(client2, func() error { return nil }, nil).
		Times(1)

	client2.EXPECT().
		GetServiceInfo(gomock.Any(), &discoveryv1.GetServiceInfoRequest{}).
		Return(&discoveryv1.GetServiceInfoResponse{
			Service: &discoveryv1.Service{
				Name: "service2",
			},
		}, nil).
		Times(1)

	job := scraper.NewJob(lister, discoverFactory)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := job.Scrape(ctx)
	assert.NilError(t, err)

	assert.DeepEqual(t, result, map[string]*discoveryv1.Service{
		"pod1": &discoveryv1.Service{
			Name: "service1",
		},
		"pod2": &discoveryv1.Service{
			Name: "service2",
		},
	})
}
