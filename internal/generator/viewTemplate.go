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
	viewTemplate = template.Must(template.New("").Parse(templates.View))
)

type viewOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
	Type        string
}

func generateView(writer io.Writer, view *viewOptions) error {
	err := viewTemplate.Execute(writer, view)
	if err != nil {
		return errors.Wrap(err, "failed to execute view template")
	}
	return nil
}

func buildViewOptions(pkg string, mod string, modelsPath string, service *models.Service, view models.View) (*viewOptions, error) {
	options := &viewOptions{
		Package: pkg,
	}

	options.TopicName = view.ToTopicName(service)
	options.Name = view.ToSafeMessageTypeName()

	topicType := "protobuf"
	if view.TopicDefinition.Type != nil {
		switch *view.TopicDefinition.Type {
		case "raw":
			topicType = "raw"
		}
	}
	options.Type = topicType

	switch topicType {
	case "protobuf":
		options.Import = fmt.Sprintf("\"%s\"", view.ToPackage(service))
		options.MessageType = view.ToMessageTypeWithPackage()

	case "raw":
		options.Import = "gokaCodecs \"github.com/lovoo/goka/codec\""
	}

	return options, nil
}
