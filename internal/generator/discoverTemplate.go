package generator

import (
	"fmt"
	"io"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"
	"github.com/syncromatics/kafmesh/pkg/runner"

	"github.com/pkg/errors"
)

var (
	discoverTemplate = template.Must(template.New("").Parse(templates.Discover))
)

type serviceDiscoveryOptions struct {
	Name        string
	Description string
}

type componentDiscoveryOptions struct {
	Name        string
	Description string
}

type sourceDiscoveryOptions struct {
	Service    *serviceDiscoveryOptions
	Component  *componentDiscoveryOptions
	Source     runner.TopicDiscovery
	MethodName string
}

type sinkDiscoveryOptions struct {
	Service     *serviceDiscoveryOptions
	Component   *componentDiscoveryOptions
	Source      runner.TopicDiscovery
	MethodName  string
	Name        string
	Description string
}

type processorDiscoveryOptions struct {
	Name        string
	Description string
	MethodName  string
	GroupName   string
	Service     *serviceDiscoveryOptions
	Component   *componentDiscoveryOptions
	Inputs      []runner.InputDiscovery
	Joins       []runner.JoinDiscovery
	Lookups     []runner.LookupDiscovery
	Outputs     []runner.OutputDiscovery
	Persistence *runner.PersistentDiscovery
}

type viewDiscoveryOptions struct {
	runner.TopicDiscovery
	Service    *serviceDiscoveryOptions
	Component  *componentDiscoveryOptions
	MethodName string
}

type viewSourceDiscoveryOptions struct {
	Service     *serviceDiscoveryOptions
	Component   *componentDiscoveryOptions
	Source      runner.TopicDiscovery
	MethodName  string
	Name        string
	Description string
}

type viewSinkDiscoveryOptions struct {
	Service     *serviceDiscoveryOptions
	Component   *componentDiscoveryOptions
	Source      runner.TopicDiscovery
	MethodName  string
	Name        string
	Description string
}

type discoverOptions struct {
	ServiceName        string
	ServiceDescription string
	Package            string
	Processors         []processorDiscoveryOptions
	Sources            []sourceDiscoveryOptions
	Sinks              []sinkDiscoveryOptions
	Views              []viewDiscoveryOptions
	ViewSources        []viewSourceDiscoveryOptions
	ViewSinks          []viewSinkDiscoveryOptions
}

func generateDiscover(writer io.Writer, service *models.Service, components []*models.Component) error {
	c := discoverOptions{
		ServiceName:        service.Name,
		ServiceDescription: service.Description,
		Processors:         []processorDiscoveryOptions{},
		Package:            service.Output.Package,
	}

	s := &serviceDiscoveryOptions{
		Name:        service.Name,
		Description: service.Description,
	}

	for _, component := range components {
		com := &componentDiscoveryOptions{
			Name:        component.Name,
			Description: component.Description,
		}

		for _, processor := range component.Processors {
			proc := processorDiscoveryOptions{
				Service:     s,
				Component:   com,
				Name:        processor.Name,
				Description: processor.Description,
				GroupName:   processor.GroupName(service, component),
				MethodName:  fmt.Sprintf("%s_%s_Processor", component.ToSafeName(), processor.ToSafeName()),
			}

			for _, input := range processor.Inputs {
				t, err := getDiscoveryTopicType(service, input.Type)
				if err != nil {
					return errors.Wrapf(err, "failed getting message type for input '%s'", input.Message)
				}
				proc.Inputs = append(proc.Inputs, runner.InputDiscovery{
					TopicDiscovery: runner.TopicDiscovery{
						Message: input.ToFullMessageType(service),
						Topic:   input.ToTopicName(service),
						Type:    t,
					},
				})
			}

			for _, join := range processor.Joins {
				t, err := getDiscoveryTopicType(service, join.Type)
				if err != nil {
					return errors.Wrapf(err, "failed getting message type for join '%s'", join.Message)
				}
				proc.Joins = append(proc.Joins, runner.JoinDiscovery{
					TopicDiscovery: runner.TopicDiscovery{
						Message: join.ToFullMessageType(service),
						Topic:   join.ToTopicName(service),
						Type:    t,
					},
				})
			}

			for _, lookup := range processor.Lookups {
				t, err := getDiscoveryTopicType(service, lookup.Type)
				if err != nil {
					return errors.Wrapf(err, "failed getting message type for lookup '%s'", lookup.Message)
				}
				proc.Lookups = append(proc.Lookups, runner.LookupDiscovery{
					TopicDiscovery: runner.TopicDiscovery{
						Message: lookup.ToFullMessageType(service),
						Topic:   lookup.ToTopicName(service),
						Type:    t,
					},
				})
			}

			for _, output := range processor.Outputs {
				t, err := getDiscoveryTopicType(service, output.Type)
				if err != nil {
					return errors.Wrapf(err, "failed getting message type for output '%s'", output.Message)
				}
				proc.Outputs = append(proc.Outputs, runner.OutputDiscovery{
					TopicDiscovery: runner.TopicDiscovery{
						Message: output.ToFullMessageType(service),
						Topic:   output.ToTopicName(service),
						Type:    t,
					},
				})
			}

			if processor.Persistence != nil {
				t, err := getDiscoveryTopicType(service, processor.Persistence.Type)
				if err != nil {
					return errors.Wrapf(err, "failed getting message type for persistence '%s'", processor.Persistence.Message)
				}

				proc.Persistence = &runner.PersistentDiscovery{
					TopicDiscovery: runner.TopicDiscovery{
						Message: processor.Persistence.ToFullMessageType(service),
						Topic:   processor.GroupName(service, component) + "-table",
						Type:    t,
					},
				}
			}

			c.Processors = append(c.Processors, proc)

		}

		for _, source := range component.Sources {
			t, err := getDiscoveryTopicType(service, source.Type)
			if err != nil {
				return errors.Wrapf(err, "failed getting message type for source '%s'", source.Message)
			}

			c.Sources = append(c.Sources, sourceDiscoveryOptions{
				Service:   s,
				Component: com,
				Source: runner.TopicDiscovery{
					Message: source.ToFullMessageType(service),
					Topic:   source.ToTopicName(service),
					Type:    t,
				},
				MethodName: fmt.Sprintf("%s_%s_Source", component.ToSafeName(), source.ToSafeMessageTypeName()),
			})
		}

		for _, sink := range component.Sinks {
			t, err := getDiscoveryTopicType(service, sink.Type)
			if err != nil {
				return errors.Wrapf(err, "failed getting message type for sink '%s'", sink.Message)
			}

			c.Sinks = append(c.Sinks, sinkDiscoveryOptions{
				Service:   s,
				Component: com,
				Source: runner.TopicDiscovery{
					Message: sink.ToFullMessageType(service),
					Topic:   sink.ToTopicName(service),
					Type:    t,
				},
				MethodName:  fmt.Sprintf("%s_%s_Sink", component.ToSafeName(), sink.ToSafeName()),
				Name:        sink.Name,
				Description: sink.Description,
			})
		}

		for _, view := range component.Views {
			t, err := getDiscoveryTopicType(service, view.Type)
			if err != nil {
				return errors.Wrapf(err, "failed getting message type for view '%s'", view.Message)
			}

			c.Views = append(c.Views, viewDiscoveryOptions{
				Service:   s,
				Component: com,
				TopicDiscovery: runner.TopicDiscovery{
					Message: view.ToFullMessageType(service),
					Topic:   view.ToTopicName(service),
					Type:    t,
				},
				MethodName: fmt.Sprintf("%s_%s_View", component.ToSafeName(), view.ToSafeMessageTypeName()),
			})
		}

		for _, viewSource := range component.ViewSources {
			t, err := getDiscoveryTopicType(service, viewSource.Type)
			if err != nil {
				return errors.Wrapf(err, "failed getting message type for viewSource '%s'", viewSource.Name)
			}

			c.ViewSources = append(c.ViewSources, viewSourceDiscoveryOptions{
				Service:   s,
				Component: com,
				Source: runner.TopicDiscovery{
					Message: viewSource.ToFullMessageType(service),
					Topic:   viewSource.ToTopicName(service),
					Type:    t,
				},
				MethodName:  fmt.Sprintf("%s_%s_ViewSource", component.ToSafeName(), viewSource.ToSafeName()),
				Name:        viewSource.Name,
				Description: viewSource.Description,
			})
		}

		for _, viewSink := range component.ViewSinks {
			t, err := getDiscoveryTopicType(service, viewSink.Type)
			if err != nil {
				return errors.Wrapf(err, "failed getting message type for viewSink '%s'", viewSink.Name)
			}

			c.ViewSinks = append(c.ViewSinks, viewSinkDiscoveryOptions{
				Service:   s,
				Component: com,
				Source: runner.TopicDiscovery{
					Message: viewSink.ToFullMessageType(service),
					Topic:   viewSink.ToTopicName(service),
					Type:    t,
				},
				MethodName:  fmt.Sprintf("%s_%s_ViewSink", component.ToSafeName(), viewSink.ToSafeName()),
				Name:        viewSink.Name,
				Description: viewSink.Description,
			})
		}
	}

	err := discoverTemplate.Execute(writer, c)
	if err != nil {
		return errors.Wrap(err, "failed to execute service template")
	}

	return nil
}

func getDiscoveryTopicType(service *models.Service, t *string) (runner.MessageType, error) {
	messageType := service.Defaults.Type
	if t != nil {
		messageType = *t
	}
	if messageType == "" {
		messageType = "protobuf"
	}

	switch messageType {
	case "protobuf":
		return runner.MessageTypeProtobuf, nil
	case "raw":
		return runner.MessageTypeRaw, nil
	default:
		return -1, errors.Errorf("unknown message type '%s'", messageType)

	}
}
