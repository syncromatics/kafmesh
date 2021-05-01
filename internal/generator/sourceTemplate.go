package generator

import (
	"fmt"
	"io"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

var (
	sourceTemplate = template.Must(template.New("").Parse(templates.Source))
)

type sourceOptions struct {
	Package       string
	Import        string
	Name          string
	TopicName     string
	MessageType   string
	ComponentName string
	ServiceName   string
	Type          string
}

func generateSource(writer io.Writer, source *sourceOptions) error {
	err := sourceTemplate.Execute(writer, source)
	if err != nil {
		return errors.Wrap(err, "failed to execute source template")
	}
	return nil
}

func buildSourceOptions(pkg string, mod string, modelsPath string, service *models.Service, component *models.Component, source models.Source) (*sourceOptions, error) {
	options := &sourceOptions{
		Package: pkg,
	}

	options.TopicName = source.ToTopicName(service)
	options.Name = source.ToSafeMessageTypeName()
	options.ServiceName = service.Name
	options.ComponentName = component.Name

	topicType := "protobuf"
	if source.TopicDefinition.Type != nil {
		switch *source.TopicDefinition.Type {
		case "raw":
			topicType = "raw"
		}
	}
	options.Type = topicType

	switch topicType {
	case "protobuf":
		options.MessageType = source.ToMessageTypeWithPackage()
		options.Import = fmt.Sprintf("\"%s\"", source.ToPackage(service))

	case "raw":
		options.Import = "gokaCodecs \"github.com/lovoo/goka/codec\""
	}

	return options, nil
}
