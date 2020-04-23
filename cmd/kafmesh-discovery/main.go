package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/syncromatics/kafmesh/internal/scraper"
	"github.com/syncromatics/kafmesh/internal/services"
	"github.com/syncromatics/kafmesh/internal/storage"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"

	_ "github.com/syncromatics/kafmesh/internal/storage/statik"
)

func main() {
	settings, err := getSettings()
	if err != nil {
		log.Fatal(err)
	}

	err = settings.DatabaseSettings.WaitForDatabaseToBeOnline(30)
	if err != nil {
		log.Fatal(err)
	}

	err = settings.DatabaseSettings.MigrateUpWithStatik("/")
	if err != nil {
		log.Fatal(err)
	}

	db, err := settings.DatabaseSettings.EnsureDatabaseExistsAndGetConnection()
	if err != nil {
		log.Fatal(err)
	}

	kubeAPIClient, err := kubernetes.NewForConfig(settings.KubernetesConfig)
	if err != nil {
		log.Fatal(err)
	}
	clientFactory := &scraper.ClientFactory{}
	scraper := scraper.NewJob(kubeAPIClient.CoreV1().Pods(""), clientFactory)

	retriever := storage.NewRetriever(db)
	updater := storage.NewUpdater(db)
	deleter := storage.NewDeleter(db)

	scraperService := services.NewScrapeService(scraper, retriever, updater, deleter, 2*time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	fmt.Println("starting scraper service go-routine...")
	group.Go(scraperService.Run(ctx))

	eventChan := make(chan os.Signal)
	signal.Notify(eventChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-eventChan:
	case <-ctx.Done():
	}

	cancel()

	if err := group.Wait(); err != nil {
		log.Fatal(err)
	}
}
