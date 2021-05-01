package generator

import (
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
)

var (
	viewSinkTemplate = template.Must(template.New("").Parse(templates.ViewSink))
)

type viewSinkOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
	Type        string
}

func generateViewSink(writer io.Writer, viewSink *viewSinkOptions) error {
	err := viewSinkTemplate.Execute(writer, viewSink)
	if err != nil {
		return errors.Wrap(err, "failed to execute viewSink template")
	}
	return nil
}

func buildViewSinkOptions(pkg string, mod string, modelsPath string, service *models.Service, viewSink models.ViewSink) (*viewSinkOptions, error) {
	options := &viewSinkOptions{
		Package: pkg,
		Name:    viewSink.ToSafeName(),
	}

	var name strings.Builder
	nameFrags := strings.Split(viewSink.Message, ".")
	for _, f := range nameFrags[1:] {
		name.WriteString(strcase.ToCamel(f))
	}

	options.TopicName = viewSink.ToTopicName(service)

	topicType := "protobuf"
	if viewSink.TopicDefinition.Type != nil {
		switch *viewSink.TopicDefinition.Type {
		case "raw":
			topicType = "raw"
		}
	}
	options.Type = topicType

	switch topicType {
	case "protobuf":
		options.Import = fmt.Sprintf("\"%s\"", viewSink.ToPackage(service))
		options.MessageType = nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])

	case "raw":
		options.Import = "gokaCodecs \"github.com/lovoo/goka/codec\""
	}

	return options, nil
}
