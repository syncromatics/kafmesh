package services_test

import (
	"context"
	"sync"
	"testing"
	"time"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	gomock "github.com/golang/mock/gomock"
	"golang.org/x/sync/errgroup"
)

func Test_ScraperService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	wg.Add(2)

	job := NewMockScraper(ctrl)

	job.EXPECT().
		Scrape(gomock.Any()).
		Return(map[string]*discoveryv1.Service{}, nil).
		DoAndReturn(func(context.Context) (map[string]*discoveryv1.Service, error) {
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
