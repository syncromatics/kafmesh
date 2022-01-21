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
	viewSourceTemplate = template.Must(template.New("").Parse(templates.ViewSource))
)

type viewSourceOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
	Type        string
}

func generateViewSource(writer io.Writer, viewSource *viewSourceOptions) error {
	err := viewSourceTemplate.Execute(writer, viewSource)
	if err != nil {
		return errors.Wrap(err, "failed to execute viewSource template")
	}
	return nil
}

func buildViewSourceOptions(pkg string, mod string, modelsPath string, service *models.Service, viewSource models.ViewSource) (*viewSourceOptions, error) {
	options := &viewSourceOptions{
		Package: pkg,
		Name:    viewSource.ToSafeName(),
	}

	nameFrags := strings.Split(viewSource.Message, ".")

	topicType := "protobuf"
	if viewSource.TopicDefinition.Type != nil {
		switch *viewSource.TopicDefinition.Type {
		case "raw":
			topicType = "raw"
		}
	}
	options.Type = topicType

	switch topicType {
	case "protobuf":
		options.Import = fmt.Sprintf("\"%s\"", viewSource.ToPackage(service))
		options.MessageType = nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])

	case "raw":
		options.Import = "gokaCodecs \"github.com/lovoo/goka/codec\""
	}

	options.TopicName = viewSource.ToTopicName(service)

	return options, nil
}
