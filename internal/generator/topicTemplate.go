package generator

import (
	"io"
	"sort"
	"text/template"
	"time"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"
	"github.com/syncromatics/kafmesh/pkg/runner"

	"github.com/pkg/errors"
)

var (
	topicTemplate = template.Must(template.New("").Parse(templates.Topic))
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
				name := input.ToTopicName(service)
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			for _, output := range p.Outputs {
				name := output.ToTopicName(service)
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
				name := join.ToTopicName(service)
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			for _, lookup := range p.Lookups {
				name := lookup.ToTopicName(service)
				topic, ok := topics[name]
				if !ok {
					topic = &topicDefinition{}
					topics[name] = topic
				}
			}

			if p.Persistence == nil {
				continue
			}

			name := p.GroupName(service, c) + "-table"
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

		for _, e := range c.Sources {
			name := e.ToTopicName(service)
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
			name := v.ToTopicName(service)
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}
		}

		for _, s := range c.Sinks {
			name := s.ToTopicName(service)
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}
		}

		for _, s := range c.ViewSources {
			name := s.ToTopicName(service)
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
			}

			compact := true
			s.TopicCreationDefinition.Compact = &compact

			err := updateTopicCreate(topic, s.TopicCreationDefinition)
			if err != nil {
				return nil, err
			}
		}

		for _, s := range c.ViewSinks {
			name := s.ToTopicName(service)
			topic, ok := topics[name]
			if !ok {
				topic = &topicDefinition{}
				topics[name] = topic
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

		if tp.Compact == nil {
			topic.Compact = false
		} else {
			topic.Compact = *tp.Compact
		}
	}

	sort.Slice(t, func(i, j int) bool {
		return t[i].Name < t[j].Name
	})

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
