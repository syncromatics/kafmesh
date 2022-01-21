package runner

import (
	"sync"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
)

var (
	serviceSetup sync.Once
)

// ServiceDiscovery provides service information for discovery
type ServiceDiscovery struct {
	Name        string
	Description string
}

// ComponentDiscovery provides component information for discovery
type ComponentDiscovery struct {
	Name        string
	Description string
}

// MessageType is the type of serialization used for the kafka topic
type MessageType int

const (
	// MessageTypeProtobuf uses protobuf serialization
	MessageTypeProtobuf MessageType = iota
	// MessageTypeRaw uses no serialization and will return the raw byte slice
	MessageTypeRaw
)

// TopicDiscovery provides topic information for discovery
type TopicDiscovery struct {
	Message string
	Topic   string
	Type    MessageType
}

// SourceDiscovery provides source information for discovery
type SourceDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery
}

// SinkDiscovery provides sink information for discovery
type SinkDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

// ViewDiscovery adds view information for discovery
type ViewDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery
}

// ViewSourceDiscovery provides view source information for discovery
type ViewSourceDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

// ViewSinkDiscovery provides view sink information for discovery
type ViewSinkDiscovery struct {
	ServiceDiscovery
	ComponentDiscovery
	TopicDiscovery

	Name        string
	Description string
}

// ProcessorDiscovery provides processor information for discovery
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

// InputDiscovery provides input information for discovery
type InputDiscovery struct {
	TopicDiscovery
}

// JoinDiscovery provides join information for discovery
type JoinDiscovery struct {
	TopicDiscovery
}

// LookupDiscovery provides lookup information for discovery
type LookupDiscovery struct {
	TopicDiscovery
}

// OutputDiscovery provides output information for discovery
type OutputDiscovery struct {
	TopicDiscovery
}

// PersistentDiscovery provides persistence information for discovery
type PersistentDiscovery struct {
	TopicDiscovery
}

func (s *Service) registerService(service ServiceDiscovery) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	serviceSetup.Do(func() {
		s.DiscoverInfo.Name = service.Name
		s.DiscoverInfo.Description = service.Description
	})
}

// RegisterProcessor registers a processor with the discovery service
func (s *Service) RegisterProcessor(processor ProcessorDiscovery) error {

	s.registerService(processor.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

// RegisterSource registers a source with the discovery service
func (s *Service) RegisterSource(source SourceDiscovery) error {

	s.registerService(source.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

// RegisterSink registers a sink with the discovery service
func (s *Service) RegisterSink(sink SinkDiscovery) error {

	s.registerService(sink.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

// RegisterView registers a view with the discovery service
func (s *Service) RegisterView(view ViewDiscovery) error {

	s.registerService(view.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

// RegisterViewSource registers a view source with the discovery service
func (s *Service) RegisterViewSource(viewSource ViewSourceDiscovery) error {

	s.registerService(viewSource.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

// RegisterViewSink registers a view sink with the discovery service
func (s *Service) RegisterViewSink(viewSink ViewSinkDiscovery) error {

	s.registerService(viewSink.ServiceDiscovery)

	s.mtx.Lock()
	defer s.mtx.Unlock()

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
	case MessageTypeRaw:
		return discoveryv1.TopicType_TOPIC_TYPE_RAW, nil
	}

	return discoveryv1.TopicType_TOPIC_TYPE_INVALID, errors.Errorf("unknown message type '%d'", messageType)
}

func (s *Service) getOrCreateDiscoveryComponent(component ComponentDiscovery) *discoveryv1.Component {
	for _, c := range s.DiscoverInfo.Components {
		if c.Name == component.Name {
			return c
		}
	}

	c := &discoveryv1.Component{
		Name:        component.Name,
		Description: component.Description,
	}

	s.DiscoverInfo.Components = append(s.DiscoverInfo.Components, c)
	return c
}
