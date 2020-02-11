package models_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/pkg/models"

	"github.com/stretchr/testify/assert"
)

func Test_ServiceDefinitionsParse(t *testing.T) {
	schema := `---
name: kafmesh
description: Kafmesh service is an example service to test kafmesh.

output:
  package: service
  path: ./internal/testing/service

messages:
  protobuf:
    - ./protos
  avro:
    - ./avro

components:
  - ./components/*.yaml

defaults:
  partition: 10
  replication: 3
  type : "protobuf"
  retention: 240h
  segment: 24h
`

	service, err := models.ParseService(bytes.NewBuffer([]byte(schema)))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, &models.Service{
		Name:        "kafmesh",
		Description: "Kafmesh service is an example service to test kafmesh.",
		Output: models.OutputSettings{
			Package: "service",
			Path:    "./internal/testing/service",
		},
		Messages: models.MessageDefinitions{
			Protobuf: []string{
				"./protos",
			},
			Avro: []string{
				"./avro",
			},
		},
		Components: []string{
			"./components/*.yaml",
		},
		Defaults: models.TopicDefaults{
			Partition:   10,
			Replication: 3,
			Type:        "protobuf",
			Retention:   10 * 24 * time.Hour,
			Segment:     24 * time.Hour,
		},
	}, service)
}

func Test_Service_ToTopicName(t *testing.T) {
	service := &models.Service{
		Name: "enplug service",
	}

	name := service.ToTopicName()
	assert.Equal(t, "enplugService", name)
}
