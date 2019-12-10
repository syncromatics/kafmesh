package models

import (
	"io"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// Component is a piece of a service that provides processors that accomplish a task
type Component struct {
	Name        string
	Description string

	Emitters      []Emitter
	Processors    []Processor
	Sinks         []Sink
	Synchronizers []Synchronizer

	Persistence *Persistence
}

// TopicDefinition describes how to get a topic name and the type of message its record is
type TopicDefinition struct {
	Message string
	Type    *string
	Topic   *string
}

// TopicCreationDefinition describe how a topic should be created
type TopicCreationDefinition struct {
	Partitions *int
	Replicas   *int
}

// Emitter is a producer into kafka
type Emitter struct {
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
}

// Processor processes kafka messages backed by a consumer group and sometimes with persistence
type Processor struct {
	GroupName   string `yaml:"groupName"`
	Description string

	Inputs  []Input
	Lookups []Lookup
	Joins   []Join
	Outputs []Output

	Persistence *Persistence
}

// Input is an edge of a processor that will take in messages from a topic
type Input struct {
	TopicDefinition `yaml:",inline"`
}

// Lookup is an edge of a processor that takes in a whole topic into a local key value database
// and makes it available to the processor by key
type Lookup struct {
	TopicDefinition `yaml:",inline"`
}

// Join is an edge of a processor that takes in a co-partitioned topic and provides the value to
// the processor by the key of the message
type Join struct {
	TopicDefinition `yaml:",inline"`
}

// Output is an edge of a processor that outputs into a kafka topic
type Output struct {
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
	Description             string
}

// Persistence is where the processor stores state data
type Persistence struct {
	TopicDefinition `yaml:",inline"`
}

// Sink is a job that will sink a topic to an external source
type Sink struct {
	Name            string
	Description     string
	TopicDefinition `yaml:",inline"`
}

// Synchronizer is a job that will sync an external source into kafka
type Synchronizer struct {
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
	Description             string
}

// ParseComponent will parse the yaml into a component
func ParseComponent(reader io.Reader) (*Component, error) {
	component := &Component{}

	d := yaml.NewDecoder(reader)
	err := d.Decode(component)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode component yaml")
	}

	return component, nil
}
