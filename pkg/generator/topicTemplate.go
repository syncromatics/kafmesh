package generator

import (
	"io"
	"text/template"
	"time"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/models"
	"github.com/syncromatics/kafmesh/pkg/runner"
)

var (
	topicTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
	"time"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
)

var (
	topics = []runner.Topic{
		{{- range .Topics }}
		runner.Topic {
			Name       : "{{ .Name }}",
			Partitions : {{ .Partitions}},
			Replicas   : {{ .Replicas }},
			Compact    : {{ .Compact }},
			Retention  : {{ .Retention.Milliseconds }}*time.Millisecond,
			Segment    : {{ .Segment.Milliseconds }}*time.Millisecond,
			Create     : {{ .Create }},
		},
		{{- end }}
	}
)

func ConfigureTopics(ctx context.Context, brokers []string) error {
	return runner.ConfigureTopics(ctx, brokers, topics)
}
`))
)

type topicDefinition struct {
	Name       string
	Partitions *int
	Replicas   *int
	Compact    *bool
	Retention  *time.Duration
	Segment    *time.Duration
	Create     bool
}

type topicOptions struct {
	Package string
	Topics  []*runner.Topic
}

func generateTopics(writer io.Writer, topic *topicOptions) error {
	err := topicTemplate.Execute(writer, topic)
	if err != nil {
		return errors.Wrap(err, "failed to execute topic template")
	}
	return nil
}

func buildTopicOption(service *models.Service, components []*models.Component) (*topicOptions, error) {
	topics := map[string]*topicDefinition{}

	for _, c := range components {
		for _, p := range c.Processors {
			for _, input := range p.Inputs {
				name := input.ToTopicName()
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			for _, output := range p.Outputs {
				name := output.ToTopicName()
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}

				err := updateTopicCreate(topic, output.TopicCreationDefinition)
				if err != nil {
					return nil, err
				}
			}

			for _, join := range p.Joins {
				name := join.ToTopicName()
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			for _, lookup := range p.Lookups {
				name := lookup.ToTopicName()
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			if p.Persistence == nil {
				continue
			}

			name := p.GroupName + "-table"
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}

			compact := true
			p.Persistence.TopicCreationDefinition.Compact = &compact

			err := updateTopicCreate(topic, p.Persistence.TopicCreationDefinition)
			if err != nil {
				return nil, err
			}
		}

		for _, e := range c.Emitters {
			name := e.ToTopicName()
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}

			err := updateTopicCreate(topic, e.TopicCreationDefinition)
			if err != nil {
				return nil, err
			}
		}

		for _, v := range c.Views {
			name := v.ToTopicName()
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}
		}

		for _, s := range c.Sinks {
			name := s.ToTopicName()
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}
		}

		for _, s := range c.Synchronizers {
			name := s.ToTopicName()
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}

			err := updateTopicCreate(topic, s.TopicCreationDefinition)
			if err != nil {
				return nil, err
			}
		}
	}

	t := []*runner.Topic{}
	for n, tp := range topics {
		topic := &runner.Topic{
			Name:   n,
			Create: tp.Create,
		}
		tp.Name = n
		t = append(t, topic)

		if !topic.Create {
			continue
		}

		if tp.Partitions == nil {
			topic.Partitions = service.Defaults.Partition
		} else {
			topic.Partitions = *tp.Partitions
		}

		if tp.Replicas == nil {
			topic.Replicas = service.Defaults.Replication
		} else {
			topic.Replicas = *tp.Replicas
		}

		if tp.Retention == nil {
			topic.Retention = service.Defaults.Retention
		} else {
			topic.Retention = *tp.Retention
		}

		if tp.Segment == nil {
			topic.Segment = service.Defaults.Segment
		} else {
			topic.Segment = *tp.Segment
		}
	}

	return &topicOptions{
		Package: service.Output.Package,
		Topics:  t,
	}, nil
}

func updateTopicCreate(topic *topicDefinition, definition models.TopicCreationDefinition) error {
	topic.Create = true

	if definition.Replicas != nil {
		if topic.Replicas != nil && *topic.Replicas != *definition.Replicas {
			return errors.Errorf("topic '%s' has two different replica configurations", topic.Name)
		}
		topic.Replicas = definition.Replicas
	}

	if definition.Partitions != nil {
		if topic.Partitions != nil && *topic.Partitions != *definition.Partitions {
			return errors.Errorf("topic '%s' has two different partition configurations", topic.Name)
		}

		topic.Partitions = definition.Partitions
	}

	if definition.Compact != nil {
		if topic.Compact != nil && *topic.Compact != *definition.Compact {
			return errors.Errorf("topic '%s' has two different compact configurations", topic.Name)
		}

		topic.Compact = definition.Compact
	}

	if definition.Retention != nil {
		if topic.Retention != nil && *topic.Retention != *definition.Retention {
			return errors.Errorf("topic '%s' has two different retention configurations", topic.Name)
		}

		topic.Retention = definition.Retention
	}

	if definition.Segment != nil {
		if topic.Segment != nil && *topic.Segment != *definition.Segment {
			return errors.Errorf("topic '%s' has two different segment configurations", topic.Name)
		}

		topic.Segment = definition.Segment
	}

	return nil
}
