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

func Test_Job_GetKafmeshPods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lister := NewMockPodLister(ctrl)
	discoverFactory := NewMockDiscoveryFactory(ctrl)

	pod1 := v1.Pod{
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
	}

	pod2 := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod2",
			Annotations: map[string]string{
				"kafmesh/scrape": "true",
				"kafmesh/port":   "7777",
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			PodIP: "1.1.1.2",
		},
	}

	pod3 := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod3",
			Annotations: map[string]string{
				"kafmesh/scrape": "true",
				"kafmesh/port":   "7777",
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			PodIP: "1.1.1.3",
		},
	}

	lister.EXPECT().
		List(gomock.Any(), gomock.Any()).
		Return(&v1.PodList{
			Items: []v1.Pod{
				pod1,
				pod2,
				pod3,
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

	job := scraper.NewJob(lister, discoverFactory)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pods, err := job.GetKafmeshPods(ctx)
	assert.NilError(t, err)
	assert.DeepEqual(t, pods, []v1.Pod{pod1, pod2, pod3})
}

func Test_Job_ScrapePod(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lister := NewMockPodLister(ctrl)
	discoverFactory := NewMockDiscoveryFactory(ctrl)

	client1 := NewMockDiscoveryClient(ctrl)

	discoverFactory.EXPECT().
		Client(gomock.Any(), "1.1.1.1:443").
		Return(client1, func() error { return nil }, nil).
		Times(1)

	expectedService := &discoveryv1.Service{
		Name:        "service1",
		Description: "service 1 does 1 things",
		Components: []*discoveryv1.Component{
			&discoveryv1.Component{
				Name:        "component 1",
				Description: "component 1 is in charge of the 1 processes",
				Sources: []*discoveryv1.Source{
					&discoveryv1.Source{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "the.1.topic",
							Message: "the.1.message",
						},
					},
				},
				Processors: []*discoveryv1.Processor{
					&discoveryv1.Processor{
						Name: "processor 1",
					},
				},
				Sinks: []*discoveryv1.Sink{
					&discoveryv1.Sink{
						Name: "sink 1",
					},
				},
				Views: []*discoveryv1.View{
					&discoveryv1.View{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "view.1.topic",
							Message: "view.1.message",
						},
					},
				},
				ViewSinks: []*discoveryv1.ViewSink{
					&discoveryv1.ViewSink{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "viewsink.1.topic",
							Message: "viewsink.1.message",
						},
					},
				},
				ViewSources: []*discoveryv1.ViewSource{
					&discoveryv1.ViewSource{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "viewsource.1.topic",
							Message: "viewsource.1.message",
						},
					},
				},
			},
		},
	}

	client1.EXPECT().
		GetServiceInfo(gomock.Any(), &discoveryv1.GetServiceInfoRequest{}).
		Return(&discoveryv1.GetServiceInfoResponse{
			Service: expectedService,
		}, nil).
		Times(1)

	job := scraper.NewJob(lister, discoverFactory)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	service, err := job.ScrapePod(ctx, v1.Pod{
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
	})
	assert.NilError(t, err)
	assert.DeepEqual(t, service, expectedService)
}
