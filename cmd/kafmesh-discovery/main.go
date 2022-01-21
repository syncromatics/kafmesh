package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph"
	"github.com/syncromatics/kafmesh/internal/scraper"
	"github.com/syncromatics/kafmesh/internal/services"
	"github.com/syncromatics/kafmesh/internal/storage"

	"github.com/syncromatics/go-kit/log"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"

	_ "github.com/syncromatics/kafmesh/internal/storage/statik"
)

func main() {
	settings, err := getSettings()
	if err != nil {
		log.Fatal("failed to get settings", "error", err)
	}

	err = settings.DatabaseSettings.WaitForDatabaseToBeOnline(30)
	if err != nil {
		log.Fatal("failed to wait for database", "error", err)
	}

	err = settings.DatabaseSettings.MigrateUpWithStatik("/")
	if err != nil {
		log.Fatal("failed migrate with statik", "error", err)
	}

	db, err := settings.DatabaseSettings.EnsureDatabaseExistsAndGetConnection()
	if err != nil {
		log.Fatal("failed to get database", "error", err)
	}

	kubeAPIClient, err := kubernetes.NewForConfig(settings.KubernetesConfig)
	if err != nil {
		log.Fatal("failed to get kube client", "error", err)
	}
	clientFactory := &scraper.ClientFactory{}
	scraper := scraper.NewJob(kubeAPIClient.CoreV1().Pods(""), clientFactory)

	retriever := storage.NewRetriever(db)
	updater := storage.NewUpdater(db)
	deleter := storage.NewDeleter(db)

	scraperService := services.NewScrapeService(scraper, retriever, updater, deleter, 2*time.Minute)
	graphService := graph.NewService(8084, db, kubeAPIClient.CoreV1().Pods(""))

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	log.Info("starting services")

	if settings.ShouldScan {
		log.Info("starting scrap service")
		group.Go(scraperService.Run(ctx))
	}

	group.Go(graphService.Run(ctx))

	eventChan := make(chan os.Signal, 1)
	signal.Notify(eventChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-eventChan:
	case <-ctx.Done():
	}

	log.Info("services stopping")

	cancel()

	if err := group.Wait(); err != nil {
		log.Fatal("errgroup failed", "error", err)
	}
}
