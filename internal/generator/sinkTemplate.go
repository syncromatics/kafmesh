package generator

import (
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

var (
	sinkTemplate = template.Must(template.New("").Parse(templates.Sink))
)

type sinkOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
	GroupName   string
	Type        string
}

func generateSink(writer io.Writer, sink *sinkOptions) error {
	err := sinkTemplate.Execute(writer, sink)
	if err != nil {
		return errors.Wrap(err, "failed to execute sink template")
	}
	return nil
}

func buildSinkOptions(pkg string, mod string, modelsPath string, sink models.Sink, service *models.Service, component *models.Component) (*sinkOptions, error) {
	options := &sinkOptions{
		Package: pkg,
	}

	topicType := "protobuf"
	if sink.TopicDefinition.Type != nil {
		switch *sink.TopicDefinition.Type {
		case "raw":
			topicType = "raw"
		}
	}

	options.Type = topicType
	options.TopicName = sink.ToTopicName(service)
	options.Name = sink.ToSafeName()
	options.GroupName = fmt.Sprintf("%s.%s.%s-sink", service.Name, component.Name, strings.ToLower(options.Name))

	switch topicType {
	case "protobuf":
		options.Import = fmt.Sprintf("\"%s\"", sink.ToPackage(service))
		options.MessageType = sink.ToMessageTypeWithPackage()

	case "raw":
		options.Import = "gokaCodecs \"github.com/lovoo/goka/codec\""
	}

	return options, nil
}
