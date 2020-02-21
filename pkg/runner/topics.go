package runner

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Topic is a definition for a kafka topic
type Topic struct {
	Name       string
	Partitions int
	Replicas   int
	Compact    bool
	Retention  time.Duration
	Segment    time.Duration
	Create     bool
}

// ConfigureTopics configures and checks topics in the slice passed.
func ConfigureTopics(ctx context.Context, brokers []string, topics []Topic) error {
	_, testMode := os.LookupEnv("KAFMESH_TEST_MODE")

	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	client, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return errors.Wrap(err, "failed to create cluster admin")
	}

	descriptions, err := client.ListTopics()
	if err != nil {
		return errors.Wrap(err, "failed to describe topics")
	}

	errs := []string{}

	for _, topic := range topics {
		if testMode {
			topic.Replicas = 1
			topic.Create = true
			topic.Segment = 1 * time.Hour
			topic.Retention = 1 * time.Hour
			topic.Partitions = 10
		}

		definition, exists := descriptions[topic.Name]
		if !exists && !topic.Create {
			errs = append(errs, fmt.Sprintf("topic '%s' does not exist and is not created in this service", topic.Name))
			continue
		}

		if !topic.Create {
			continue
		}

		retention := fmt.Sprintf("%d", topic.Retention/time.Millisecond)
		segment := fmt.Sprintf("%d", topic.Segment/time.Millisecond)
		config := map[string]*string{
			"retention.ms": &retention,
			"segment.ms":   &segment,
		}

		if topic.Compact {
			c := "compact"
			config["cleanup.policy"] = &c
		}

		if !exists {
			err = client.CreateTopic(topic.Name, &sarama.TopicDetail{
				NumPartitions:     int32(topic.Partitions),
				ReplicationFactor: int16(topic.Replicas),
				ConfigEntries:     config,
			}, false)
			if err != nil && strings.Contains(err.Error(), "Topic with this name already exists") {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "failed to create topic")
			}
			continue
		}

		if definition.NumPartitions != int32(topic.Partitions) {
			errs = append(errs, fmt.Sprintf("topic '%s' is configured with '%d' partitions and cannot be change to '%d' partitions", topic.Name, definition.NumPartitions, topic.Partitions))
			continue
		}

		shouldUpdate := false
		for k, v := range config {
			cv, ok := definition.ConfigEntries[k]
			if !ok || cv != v {
				shouldUpdate = true
			}
		}

		for k := range definition.ConfigEntries {
			_, ok := config[k]
			if !ok {
				shouldUpdate = true
			}
		}

		if !shouldUpdate {
			continue
		}

		err = client.AlterConfig(sarama.TopicResource, topic.Name, config, false)
		if err != nil {
			return errors.Wrapf(err, "failed to alert config on topic '%s'", topic.Name)
		}
	}

	if len(errs) > 0 {
		return errors.Errorf("topic configuration invalid '%s'", strings.Join(errs, ","))
	}

	return nil
}
