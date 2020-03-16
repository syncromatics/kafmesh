package models

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var (
	capitalsRegex = regexp.MustCompile(`[A-Z][^A-Z]*`)
)

// Component is a piece of a service that provides processors that accomplish a task
type Component struct {
	Name        string
	Description string

	Sources     []Source
	Processors  []Processor
	Sinks       []Sink
	Views       []View
	ViewSources []ViewSource `yaml:"viewSources"`
	ViewSinks   []ViewSink   `yaml:"viewSinks"`

	Persistence *Persistence
}

// ToGroupName converts the component name to a kafka safe name to be used in group names
func (c *Component) ToGroupName() string {
	builder := strings.Builder{}
	first := true
	for _, f := range strings.Split(c.Name, " ") {
		if first {
			builder.WriteString(strcase.ToLowerCamel(f))
			first = false
			continue
		}
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
}

// ToSafeName converts the component name to a go safe name
func (c *Component) ToSafeName() string {
	builder := strings.Builder{}
	for _, f := range strings.Split(c.Name, " ") {
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
}

// TopicDefinition describes how to get a topic name and the type of message its record is
type TopicDefinition struct {
	Message string
	Type    *string
	Topic   *string
}

// ToTopicName extracts the topic name from the definition
func (t TopicDefinition) ToTopicName(service *Service) string {
	if t.Topic != nil {
		return *t.Topic
	}

	return t.ToFullMessageType(service)
}

// ToFullMessageType extracts the full message type from the definition
func (t TopicDefinition) ToFullMessageType(service *Service) string {
	builder := strings.Builder{}
	for _, f := range strings.Split(t.Message, ".") {
		builder.WriteString(strcase.ToLowerCamel(f))
		builder.WriteString(".")
	}

	m := strings.TrimRight(builder.String(), ".")

	if len(strings.Split(t.Message, ".")) == 2 {
		return fmt.Sprintf("%s.%s", service.ToTopicName(), m)
	}

	return m
}

// ToSafeMessageTypeName generates a name that will pass go vet
func (t TopicDefinition) ToSafeMessageTypeName() string {
	builder := strings.Builder{}
	nameFrags := strings.Split(t.Message, ".")
	for _, f := range nameFrags {
		builder.WriteString(strcase.ToCamel(f))
	}

	name := builder.String()
	builder.Reset()

	submatchall := capitalsRegex.FindAllString(name, -1)
	for _, element := range submatchall {
		switch element {
		case "Id":
			builder.WriteString("ID")
		case "Api":
			builder.WriteString("API")
		default:
			builder.WriteString(element)
		}

	}
	return builder.String()
}

// ToPackage gets the package of the topic message
func (t TopicDefinition) ToPackage(service *Service) string {
	builder := strings.Builder{}
	builder.WriteString(service.Output.Module)
	builder.WriteString("/")
	builder.WriteString(service.Output.Path)
	builder.WriteString("/models/")

	frags := strings.Split(t.ToFullMessageType(service), ".")
	for _, f := range frags[:len(frags)-1] {
		builder.WriteString(f)
		builder.WriteString("/")
	}

	return strings.TrimRight(builder.String(), "/")
}

// ToMessageTypeWithPackage gets the message type with package
func (t TopicDefinition) ToMessageTypeWithPackage() string {
	nameFrags := strings.Split(t.Message, ".")

	return nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])
}

// TopicCreationDefinition describe how a topic should be created
type TopicCreationDefinition struct {
	Partitions *int
	Replicas   *int
	Compact    *bool
	Retention  *time.Duration
	Segment    *time.Duration
}

// Source is a producer into kafka
type Source struct {
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
}

// View is a view into kafka
type View struct {
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
}

// Processor processes kafka messages backed by a consumer group and sometimes with persistence
type Processor struct {
	Name              string
	GroupNameOverride *string `yaml:"groupName"`
	Description       string

	Inputs  []Input
	Lookups []Lookup
	Joins   []Join
	Outputs []Output

	Persistence *Persistence
}

// ToSafeName get a go safe name
func (p *Processor) ToSafeName() string {
	builder := strings.Builder{}
	for _, f := range strings.Split(p.Name, " ") {
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
}

// GroupName gets the  consumer group name of the topic
func (p *Processor) GroupName(service *Service, component *Component) string {
	if p.GroupNameOverride != nil {
		return *p.GroupNameOverride
	}

	return fmt.Sprintf("%s.%s.%s", service.ToTopicName(), component.ToGroupName(), strcase.ToLowerCamel(p.Name))
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
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
}

// Sink is a job that will sink a topic to an external source
type Sink struct {
	Name            string
	Description     string
	TopicDefinition `yaml:",inline"`
}

// ToSafeName get a go safe name
func (p *Sink) ToSafeName() string {
	builder := strings.Builder{}
	for _, f := range strings.Split(p.Name, " ") {
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
}

// ViewSource is a job that will sync an external source into a kafka view
type ViewSource struct {
	Name                    string
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
	Description             string
}

// ToSafeName get a go safe name
func (p *ViewSource) ToSafeName() string {
	builder := strings.Builder{}
	for _, f := range strings.Split(p.Name, " ") {
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
}

// ViewSink is a job that will sync a kafka view to an external destination
type ViewSink struct {
	Name                    string
	TopicDefinition         `yaml:",inline"`
	TopicCreationDefinition `yaml:",inline"`
	Description             string
}

// ToSafeName get a go safe name
func (p *ViewSink) ToSafeName() string {
	builder := strings.Builder{}
	for _, f := range strings.Split(p.Name, " ") {
		builder.WriteString(strcase.ToCamel(f))
	}
	return builder.String()
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
