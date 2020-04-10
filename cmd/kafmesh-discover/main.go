package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/syncromatics/kafmesh/internal/scraper"
	"github.com/syncromatics/kafmesh/internal/services"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var kubeAPIClient *kubernetes.Clientset

const (
	scrapeAnnotation = "kafmesh/scrape"
)

func init() {

	var config *rest.Config
	var err error

	_, exists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	if !exists {
		config = getLocalConfig()
	} else {
		config = getClusterConfig()
	}

	kubeAPIClient, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	clientFactory := &scraper.ClientFactory{}
	scraper := scraper.NewJob(kubeAPIClient.CoreV1().Pods(""), clientFactory)
	scraperService := services.NewScrapeService(scraper, 2*time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	group.Go(scraperService.Run(ctx))

	eventChan := make(chan os.Signal)
	signal.Notify(eventChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-eventChan:
		fmt.Println("os.Signal received, exiting")
		cancel()
	case <-ctx.Done():
		fmt.Println("ctxDone")
	}

}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func getLocalConfig() *rest.Config {
	fmt.Println("Getting local KubeAPI config...")
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	return config
}

func getClusterConfig() *rest.Config {
	fmt.Println("Getting the in-cluster KubeAPI config...")
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	return config
}
