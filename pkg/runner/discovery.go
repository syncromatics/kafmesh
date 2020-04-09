package runner

import (
	"sync"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
)

var (
	serviceSetup sync.Once
)

type ServiceDiscovery struct {
	Name        string
	Description string
}

type ComponentDiscovery struct {
	Name        string
	Description string
}

type MessageType int

const (
	MessageTypeProtobuf MessageType = iota
	MessageTypeAvro
)

type TopicDiscovery struct {
	Message string
	Topic   string
	Type    MessageType
}

type SourceDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery
}

type SinkDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

type ViewDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery
}

type ViewSourceDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

type ViewSinkDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

type ProcessorDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery

	Name        string
	Description string
	GroupName   string

	Inputs      []InputDiscovery
	Joins       []JoinDiscovery
	Lookups     []LookupDiscovery
	Outputs     []OutputDiscovery
	Persistence *PersistentDiscovery
}

type InputDiscovery struct {
	TopicDiscovery
}

type JoinDiscovery struct {
	TopicDiscovery
}

type LookupDiscovery struct {
	TopicDiscovery
}

type OutputDiscovery struct {
	TopicDiscovery
}

type PersistentDiscovery struct {
	TopicDiscovery
}

func (s *Service) registerService(service ServiceDiscovery) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	serviceSetup.Do(func() {
		s.DiscoveryInfo.Name = service.Name
		s.DiscoveryInfo.Description = service.Description
	})
}

func (s *Service) RegisterProcessor(processor ProcessorDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(processor.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(processor.ComponentDiscovery)

	proc := &discoveryv1.Processor{
		Name:        processor.Name,
		Description: processor.Description,
		GroupName:   processor.GroupName,
	}

	for _, input := range processor.Inputs {
		t, err := convertMessageType(input.Type)
		if err != nil {
			return errors.Wrapf(err, "processor '%s' input '%s' has invalid message type", processor.Name, input.Topic)
		}

		proc.Inputs = append(proc.Inputs, &discoveryv1.Input{
			Topic: &discoveryv1.TopicDefinition{
				Message: input.Message,
				Topic:   input.Topic,
				Type:    t,
			},
		})
	}

	for _, join := range processor.Joins {
		t, err := convertMessageType(join.Type)
		if err != nil {
			return errors.Wrapf(err, "processor '%s' join '%s' has invalid message type", processor.Name, join.Topic)
		}

		proc.Joins = append(proc.Joins, &discoveryv1.Join{
			Topic: &discoveryv1.TopicDefinition{
				Message: join.Message,
				Topic:   join.Topic,
				Type:    t,
			},
		})
	}

	for _, lookup := range processor.Lookups {
		t, err := convertMessageType(lookup.Type)
		if err != nil {
			return errors.Wrapf(err, "processor '%s' lookup '%s' has invalid message type", processor.Name, lookup.Topic)
		}

		proc.Lookups = append(proc.Lookups, &discoveryv1.Lookup{
			Topic: &discoveryv1.TopicDefinition{
				Message: lookup.Message,
				Topic:   lookup.Topic,
				Type:    t,
			},
		})
	}

	for _, output := range processor.Outputs {
		t, err := convertMessageType(output.Type)
		if err != nil {
			return errors.Wrapf(err, "processor '%s' output '%s' has invalid message type", processor.Name, output.Topic)
		}

		proc.Outputs = append(proc.Outputs, &discoveryv1.Output{
			Topic: &discoveryv1.TopicDefinition{
				Message: output.Message,
				Topic:   output.Topic,
				Type:    t,
			},
		})
	}

	if processor.Persistence != nil {
		t, err := convertMessageType(processor.Persistence.Type)
		if err != nil {
			return errors.Wrapf(err, "processor '%s' persistence '%s' has invalid message type", processor.Name, processor.Persistence.Topic)
		}

		proc.Persistence = &discoveryv1.Persistence{
			Topic: &discoveryv1.TopicDefinition{
				Message: processor.Persistence.Message,
				Topic:   processor.Persistence.Topic,
				Type:    t,
			},
		}
	}

	component.Processors = append(component.Processors, proc)

	return nil
}

func (s *Service) RegisterSource(source SourceDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(source.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(source.ComponentDiscovery)

	t, err := convertMessageType(source.Type)
	if err != nil {
		return errors.Wrapf(err, "source '%s' has invalid message type", source.Topic)
	}

	src := &discoveryv1.Source{
		Topic: &discoveryv1.TopicDefinition{
			Message: source.Message,
			Topic:   source.Topic,
			Type:    t,
		},
	}

	component.Sources = append(component.Sources, src)
	return nil
}

func (s *Service) RegisterSink(sink SinkDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(sink.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(sink.ComponentDiscovery)

	t, err := convertMessageType(sink.Type)
	if err != nil {
		return errors.Wrapf(err, "sink '%s' has invalid message type", sink.Topic)
	}

	component.Sinks = append(component.Sinks, &discoveryv1.Sink{
		Topic: &discoveryv1.TopicDefinition{
			Message: sink.Message,
			Topic:   sink.Topic,
			Type:    t,
		},
		Name:        sink.Name,
		Description: sink.Description,
	})
	return nil
}

func (s *Service) RegisterView(view ViewDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(view.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(view.ComponentDiscovery)

	t, err := convertMessageType(view.Type)
	if err != nil {
		return errors.Wrapf(err, "view '%s' has invalid message type", view.Topic)
	}

	component.Views = append(component.Views, &discoveryv1.View{
		Topic: &discoveryv1.TopicDefinition{
			Message: view.Message,
			Topic:   view.Topic,
			Type:    t,
		},
	})
	return nil
}

func (s *Service) RegisterViewSource(viewSource ViewSourceDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(viewSource.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(viewSource.ComponentDiscovery)

	t, err := convertMessageType(viewSource.Type)
	if err != nil {
		return errors.Wrapf(err, "viewSource '%s' has invalid message type", viewSource.Name)
	}

	component.ViewSources = append(component.ViewSources, &discoveryv1.ViewSource{
		Topic: &discoveryv1.TopicDefinition{
			Message: viewSource.Message,
			Topic:   viewSource.Topic,
			Type:    t,
		},
		Name:        viewSource.Name,
		Description: viewSource.Description,
	})
	return nil
}

func (s *Service) RegisterViewSink(viewSink ViewSinkDiscovery) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.registerService(viewSink.ServiceDiscovery)

	component := s.getOrCreateDiscoveryComponent(viewSink.ComponentDiscovery)

	t, err := convertMessageType(viewSink.Type)
	if err != nil {
		return errors.Wrapf(err, "viewSink '%s' has invalid message type", viewSink.Name)
	}

	component.ViewSinks = append(component.ViewSinks, &discoveryv1.ViewSink{
		Topic: &discoveryv1.TopicDefinition{
			Message: viewSink.Message,
			Topic:   viewSink.Topic,
			Type:    t,
		},
		Name:        viewSink.Name,
		Description: viewSink.Description,
	})
	return nil
}

func convertMessageType(messageType MessageType) (discoveryv1.TopicType, error) {
	switch messageType {
	case MessageTypeProtobuf:
		return discoveryv1.TopicType_TOPIC_TYPE_PROTOBUF, nil

	case MessageTypeAvro:
		return discoveryv1.TopicType_TOPIC_TYPE_AVRO, nil
	}

	return discoveryv1.TopicType_TOPIC_TYPE_INVALID, errors.Errorf("unknown message type '%d'", messageType)
}

func (s *Service) getOrCreateDiscoveryComponent(component ComponentDiscovery) *discoveryv1.Component {
	for _, c := range s.DiscoveryInfo.Components {
		if c.Name == component.Name {
			return c
		}
	}

	c := &discoveryv1.Component{
		Name:        component.Name,
		Description: component.Description,
	}

	s.DiscoveryInfo.Components = append(s.DiscoveryInfo.Components, c)
	return c
}
