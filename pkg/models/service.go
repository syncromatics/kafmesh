package models

import (
	"io"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// Service is a description of a kafmesh service
type Service struct {
	Name        string
	Description string
	Components  []string
	Output      OutputSettings
	Defaults    TopicDefaults
	Messages    MessageDefinitions
}

// OutputSettings define how the service is generated
type OutputSettings struct {
	Package string
	Path    string
}

// TopicDefaults are the default kafka settings for the service
type TopicDefaults struct {
	Partition   int
	Replication int
	Type        string
}

// MessageDefinitions define where to locate the schema for the messages.
type MessageDefinitions struct {
	Protobuf []string
	Avro     []string
}

// ParseService the reader to a Service
func ParseService(reader io.Reader) (*Service, error) {
	service := &Service{}

	d := yaml.NewDecoder(reader)
	err := d.Decode(service)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode service yaml")
	}

	return service, nil
}
