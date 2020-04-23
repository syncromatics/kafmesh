package services

import (
	"context"
	"time"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	"github.com/syncromatics/kafmesh/internal/storage"

	"github.com/pkg/errors"
	"github.com/syncromatics/go-kit/log"
	v1 "k8s.io/api/core/v1"
)

//go:generate mockgen -source=./scrapeService.go -destination=./scrapeService_mock_test.go -package=services_test

// Scraper scrapes pods for kafmesh discovery info
type Scraper interface {
	ScrapePod(ctx context.Context, pod v1.Pod) (*discoveryv1.Service, error)
	GetKafmeshPods(ctx context.Context) ([]v1.Pod, error)
}

// Updater updates pods in storage
type Updater interface {
	Update(context.Context, storage.Pod, *discoveryv1.Service) error
}

// Deleter deletes pods out of storage
type Deleter interface {
	Delete(context.Context, storage.Pod) error
}

// GetPodser gets the existing pods from storage
type GetPodser interface {
	GetPods(context.Context) (map[string]struct{}, error)
}

// ScrapeService periodically scrapes pods in the k8s cluster for kafmesh info
type ScrapeService struct {
	scraper  Scraper
	storage  GetPodser
	updater  Updater
	deleter  Deleter
	interval time.Duration
}

// NewScrapeService creates a new scrape service
func NewScrapeService(scraper Scraper, storage GetPodser, updater Updater, deleter Deleter, interval time.Duration) *ScrapeService {
	return &ScrapeService{scraper, storage, updater, deleter, interval}
}

// Run the scrape service
func (s *ScrapeService) Run(ctx context.Context) func() error {
	return func() error {
		timer := time.NewTimer(0 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil

			case <-timer.C:
				err := s.Scrape(ctx)
				if err != nil {
					return err
				}

				timer = time.NewTimer(s.interval)
			}
		}
	}
}

// Scrape runs the scrape job
func (s *ScrapeService) Scrape(ctx context.Context) error {
	pods, err := s.storage.GetPods(ctx)
	if err != nil {
		return errors.Wrap(err, "failed getting pods from storage")
	}

	kPods, err := s.scraper.GetKafmeshPods(ctx)
	if err != nil {
		return errors.Wrap(err, "failed getting kafmesh pods")
	}

	seenPods := map[string]struct{}{}
	for _, pod := range kPods {
		seenPods[pod.Name] = struct{}{}

		_, ok := pods[pod.Name]
		if ok {
			continue
		}

		if pod.Status.Phase != v1.PodRunning {
			continue
		}

		service, err := s.scraper.ScrapePod(ctx, pod)
		if err != nil {
			log.Error("scraping pod failed", "pod", pod.Name)
			continue
		}

		err = s.updater.Update(ctx, storage.Pod{Name: pod.Name}, service)
		if err != nil {
			return errors.Wrap(err, "failed to insert pod service")
		}
	}

	for pod := range pods {
		_, ok := seenPods[pod]
		if ok {
			continue
		}

		err = s.deleter.Delete(ctx, storage.Pod{Name: pod})
		if err != nil {
			return errors.Wrap(err, "failed to delete pod service")
		}
	}

	return nil
}
