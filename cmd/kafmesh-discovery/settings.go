package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/syncromatics/go-kit/database"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type settings struct {
	KubernetesConfig *rest.Config
	DatabaseSettings *database.PostgresDatabaseSettings
}

func getSettings() (*settings, error) {
	var config *rest.Config
	var err error

	_, ok := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	if !ok {
		config, err = getLocalConfig()
		if err != nil {
			return nil, errors.Wrap(err, "failed getting kubernetes config")
		}
	} else {
		config, err = getClusterConfig()
		if err != nil {
			return nil, errors.Wrap(err, "failed getting kubernetes config")
		}
	}

	errors := []string{}

	ds := &database.PostgresDatabaseSettings{}
	ds.Host, ok = os.LookupEnv("DATABASE_HOST")
	if !ok {
		errors = append(errors, "DATABASE_HOST")
	}

	ds.Name, ok = os.LookupEnv("DATABASE_NAME")
	if !ok {
		errors = append(errors, "DATABASE_NAME")
	}

	ds.User, ok = os.LookupEnv("DATABASE_USER")
	if !ok {
		errors = append(errors, "DATABASE_USER")
	}

	ds.Password, ok = os.LookupEnv("DATABASE_PASSWORD")
	if !ok {
		errors = append(errors, "DATABASE_PASSWORD")
	}

	if len(errors) > 0 {
		return nil, fmt.Errorf("Missing required environment variables: %s", strings.Join(errors, ", "))
	}

	return &settings{
		KubernetesConfig: config,
		DatabaseSettings: ds,
	}, nil
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func getLocalConfig() (*rest.Config, error) {
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed building config")
	}
	return config, nil
}

func getClusterConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed getting in cluster conifg")
	}
	return config, nil
}
