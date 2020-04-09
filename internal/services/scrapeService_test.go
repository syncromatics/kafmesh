package services_test

import (
	"context"
	"sync"
	"testing"
	"time"

	discoverv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discover/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	gomock "github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"gotest.tools/assert"
)

func Test_ScraperService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	wg.Add(2)

	job := NewMockScraper(ctrl)

	job.EXPECT().
		Scrape(gomock.Any()).
		Return(map[string]*discoverv1.DiscoverResponse{}, nil).
		DoAndReturn(func(context.Context) (map[string]*discoverv1.DiscoverResponse, error) {
			wg.Add(-1)
			return nil, nil
		}).
		Times(2)

	scrapeService := services.NewScrapeService(job, 1*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	group, ctx := errgroup.WithContext(ctx)
	defer cancel()

	group.Go(scrapeService.Run(ctx))

	wgChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgChan)
	}()
	select {
	case <-ctx.Done():
	case <-wgChan:
		return
	}

	cancel()

	t.Fatal(group.Wait())
}

func Test_ScraperService_ShouldReturnErrorIfScrapeFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	job := NewMockScraper(ctrl)

	job.EXPECT().
		Scrape(gomock.Any()).
		Return(nil, errors.Errorf("boom")).
		Times(1)

	scrapeService := services.NewScrapeService(job, 1*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	group, ctx := errgroup.WithContext(ctx)
	defer cancel()

	group.Go(scrapeService.Run(ctx))

	select {
	case <-ctx.Done():
	}

	cancel()

	assert.ErrorContains(t, group.Wait(), "failed to scrape: boom")
}
