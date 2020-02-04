package models_test

import (
	"bytes"
	"testing"

	"github.com/syncromatics/kafmesh/pkg/models"

	"github.com/stretchr/testify/assert"
)

func Test_ComponentParse(t *testing.T) {
	schema := `---
name: details
description: The details component handles the flow for device details.

emitters:
  - message: kafmesh.deviceId.detail
    type: protobuf
    partitions: 10

processors:
  - groupName: kafmesh.deviceId.enrichedDetail
    description: Provides enriched device details with customer information.
    inputs:
      - message: kafmesh.deviceId.detail
        type: protobuf
      - message: kafmesh.deviceId.customer
    lookups:
      - message: kafmesh.customerId.details
        type: protobuf
    joins:
      - message: kafmesh.customerId.details
        type: protobuf
    outputs:
      - message: kafmesh.deviceId.enrichedDetail
        description: Enriched device details
        type: protobuf
        partitions: 10
    persistence:
      message: kafmesh.deviceId.enrichedDetailsState
      type: protobuf

sinks:
  - message: kafmesh.deviceId.enrichedDetail
    name: Enriched Detail Warehouse Sink
    description: Sinks enriched device details to the warehouse database.
    type: protobuf

synchronizers:
  - message: kafmesh.deviceId.customer
    type: protobuf
    description: Synchronizes the assigned devices in the database with kafka
    partitions: 10
`

	component, err := models.ParseComponent(bytes.NewBuffer([]byte(schema)))
	if err != nil {
		t.Fatal(err)
	}

	partition := 10
	topicType := "protobuf"
	assert.Equal(t, &models.Component{
		Name:        "details",
		Description: "The details component handles the flow for device details.",

		Emitters: []models.Emitter{
			models.Emitter{
				TopicDefinition: models.TopicDefinition{
					Message: "kafmesh.deviceId.detail",
					Type:    &topicType,
				},
				TopicCreationDefinition: models.TopicCreationDefinition{
					Partitions: &partition,
				},
			},
		},

		Processors: []models.Processor{
			models.Processor{
				GroupName:   "kafmesh.deviceId.enrichedDetail",
				Description: "Provides enriched device details with customer information.",

				Inputs: []models.Input{
					models.Input{
						TopicDefinition: models.TopicDefinition{
							Message: "kafmesh.deviceId.detail",
							Type:    &topicType,
						},
					},
					models.Input{
						TopicDefinition: models.TopicDefinition{
							Message: "kafmesh.deviceId.customer",
						},
					},
				},

				Lookups: []models.Lookup{
					models.Lookup{
						TopicDefinition: models.TopicDefinition{
							Message: "kafmesh.customerId.details",
							Type:    &topicType,
						},
					},
				},

				Joins: []models.Join{
					models.Join{
						TopicDefinition: models.TopicDefinition{
							Message: "kafmesh.customerId.details",
							Type:    &topicType,
						},
					},
				},

				Outputs: []models.Output{
					models.Output{
						TopicDefinition: models.TopicDefinition{
							Message: "kafmesh.deviceId.enrichedDetail",
							Type:    &topicType,
						},
						TopicCreationDefinition: models.TopicCreationDefinition{
							Partitions: &partition,
						},
						Description: "Enriched device details",
					},
				},

				Persistence: &models.Persistence{
					TopicDefinition: models.TopicDefinition{
						Message: "kafmesh.deviceId.enrichedDetailsState",
						Type:    &topicType,
					},
				},
			},
		},

		Sinks: []models.Sink{
			models.Sink{
				Name:        "Enriched Detail Warehouse Sink",
				Description: "Sinks enriched device details to the warehouse database.",
				TopicDefinition: models.TopicDefinition{
					Message: "kafmesh.deviceId.enrichedDetail",
					Type:    &topicType,
				},
			},
		},

		Synchronizers: []models.Synchronizer{
			models.Synchronizer{
				TopicDefinition: models.TopicDefinition{
					Message: "kafmesh.deviceId.customer",
					Type:    &topicType,
				},
				TopicCreationDefinition: models.TopicCreationDefinition{
					Partitions: &partition,
				},
				Description: "Synchronizes the assigned devices in the database with kafka",
			},
		},
	}, component)
}

func Test_TopicDefinition_ToSafeMessageTypeName(t *testing.T) {
	topic := models.TopicDefinition{
		Message: "deviceId.Customer",
	}

	name := topic.ToSafeMessageTypeName()
	assert.Equal(t, "DeviceIDCustomer", name)

	topic = models.TopicDefinition{
		Message: "device.Api",
	}

	name = topic.ToSafeMessageTypeName()
	assert.Equal(t, "DeviceAPI", name)
}
