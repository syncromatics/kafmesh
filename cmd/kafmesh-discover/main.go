package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	discoverv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discover/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
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
	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	group.Go(ScrapeCluster(ctx))

	eventChan := make(chan os.Signal)
	signal.Notify(eventChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-eventChan:
		fmt.Println("os.Signal received, exiting")
		cancel()
	case <-ctx.Done():
		fmt.Println("ctxDone...cancel?")
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


func getKafmeshScrapablePods() []v1.Pod {

	allPods, err := kubeAPIClient.CoreV1().Pods("").List(metav1.ListOptions{})
	if err!= nil {
		log.Fatal(err)
	}

	var scrapablePods []v1.Pod

	for _, pod := range allPods.Items {
		scrapable, ok := pod.Annotations[scrapeAnnotation]
		if ok && scrapable == "true" && pod.Status.Phase == v1.PodRunning {
			scrapablePods = append(scrapablePods, pod)
		}
	}

	return scrapablePods
}

// ScrapeCluster calls the discover GRPC method for all kafmesh enabled services
func ScrapeCluster(ctx context.Context) func() error {
	return func() error {
		for {
			scrapablePods := getKafmeshScrapablePods()
			fmt.Printf("Found %v scrapable pods...\n", len(scrapablePods))
			for _, pod := range scrapablePods {
				go (scrapePod(ctx,pod))()
			}
			time.Sleep(1 * time.Minute)
		}
	}
}

func scrapePod(ctx context.Context,pod v1.Pod) func() error {
	return func() error {
		fmt.Println("opening grpc connection for discover")

		//ToDo: force use of specific port and filter out pods for same service
		url := pod.Status.PodIP + ":443"
		con, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
		if err != nil {
			return errors.Wrap(err, "failed to dial service")
		}

		discover := discoverv1.NewDiscoverClient(con)

		//ToDo: Extract service address, no duplicates, use that for grpc url
		//fmt.Printf("%v\n", pod.ObjectMeta.String())

		fmt.Println("calling discover rpc")
		response, err := discover.GetServiceInfo(ctx, &discoverv1.DiscoverRequest{})
		if err != nil {
			return errors.Wrap(err, "error: ")
		}
		if err == context.Canceled {
			return errors.Wrap(err, "timeout: ")
		}

		fmt.Printf("Got response: -x- %v -x-\n", response)

		select {
		case <-ctx.Done():
			return errors.Wrapf(err, "context canceled, last error: ")
		default:
			return nil
		}
	}
}
