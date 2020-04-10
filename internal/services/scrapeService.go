package services

import (
	"context"
	"fmt"
	"time"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
)

//go:generate mockgen -source=./scrapeService.go -destination=./scrapeService_mock_test.go -package=services_test

// Scraper scrapes pods for kafmesh discovery info
type Scraper interface {
	Scrape(ctx context.Context) (map[string]*discoveryv1.Service, error)
}

// ScrapeService periodically scrapes pods in the k8s cluster for kafmesh info
type ScrapeService struct {
	scraper  Scraper
	interval time.Duration
}

// NewScrapeService creates a new scrape service
func NewScrapeService(scraper Scraper, interval time.Duration) *ScrapeService {
	return &ScrapeService{scraper, interval}
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
				response, err := s.scraper.Scrape(ctx)
				if err != nil {
					return errors.Wrap(err, "failed to scrape\n")
				}
				for key, val := range response {
					fmt.Printf("%v --- %v\n", key, val)
				}

				timer = time.NewTimer(s.interval)
			}
		}
	}
}
