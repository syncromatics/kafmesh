package services_test

import (
	"context"
	"testing"
	"time"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	"github.com/syncromatics/kafmesh/internal/services"
	"github.com/syncromatics/kafmesh/internal/storage"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_ScraperService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	job := NewMockScraper(ctrl)
	getter := NewMockGetPodser(ctrl)
	updater := NewMockUpdater(ctrl)
	deleter := NewMockDeleter(ctrl)

	getter.EXPECT().
		GetPods(gomock.Any()).
		Return(map[string]struct{}{
			"existingPod": struct{}{},
			"deletePod":   struct{}{},
		}, nil).
		Times(1)

	newPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "newPod",
			Annotations: map[string]string{
				"kafmesh/scrape": "true",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "1.1.1.1",
		},
	}

	job.EXPECT().
		GetKafmeshPods(gomock.Any()).
		Return([]corev1.Pod{
			corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "existingPod",
					Annotations: map[string]string{
						"kafmesh/scrape": "true",
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: "1.1.1.1",
				},
			},
			corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "failedPod",
					Annotations: map[string]string{
						"kafmesh/scrape": "true",
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
					PodIP: "1.1.1.1",
				},
			},
			newPod,
		}, nil).
		Times(1)

	job.EXPECT().
		ScrapePod(gomock.Any(), newPod).
		Return(&discoveryv1.Service{Name: "newService"}, nil).
		Times(1)

	updater.EXPECT().
		Update(gomock.Any(), storage.Pod{Name: "newPod"}, &discoveryv1.Service{Name: "newService"}).
		Return(nil).
		Times(1)

	deleter.EXPECT().
		Delete(gomock.Any(), storage.Pod{Name: "deletePod"}).
		Return(nil).
		Times(1)

	scrapeService := services.NewScrapeService(job, getter, updater, deleter, 1*time.Second)

	err := scrapeService.Scrape(context.Background())
	assert.NilError(t, err)
}

func Test_ScraperService_GetPodsShouldReturnErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	job := NewMockScraper(ctrl)
	getter := NewMockGetPodser(ctrl)
	updater := NewMockUpdater(ctrl)
	deleter := NewMockDeleter(ctrl)

	getter.EXPECT().
		GetPods(gomock.Any()).
		Return(nil, errors.Errorf("boom"))

	service := services.NewScrapeService(job, getter, updater, deleter, 1*time.Second)
	err := service.Scrape(context.Background())
	assert.ErrorContains(t, err, "failed getting pods from storage: boom")
}

func Test_ScraperService_GetKafmeshPodsShouldReturnErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	job := NewMockScraper(ctrl)
	getter := NewMockGetPodser(ctrl)
	updater := NewMockUpdater(ctrl)
	deleter := NewMockDeleter(ctrl)

	getter.EXPECT().
		GetPods(gomock.Any()).
		Return(nil, nil)

	job.EXPECT().
		GetKafmeshPods(gomock.Any()).
		Return(nil, errors.Errorf("boom"))

	service := services.NewScrapeService(job, getter, updater, deleter, 1*time.Second)
	err := service.Scrape(context.Background())
	assert.ErrorContains(t, err, "failed getting kafmesh pods: boom")
}
