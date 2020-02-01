package generator

import (
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/models"
)

var (
	emitterTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
	"context"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"

	"github.com/syncromatics/kafmesh/pkg/runner"

	{{ .Import }}
)

type {{ .Name }}_Emitter struct {
	emitter *runner.Emitter
}

type {{ .Name }}_Emitter_Message struct {
	Key string
	Value *{{ .MessageType }}
}

func New_{{ .Name }}_Emitter(options runner.ServiceOptions) (*{{ .Name }}_Emitter, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("{{ .TopicName }}", &{{ .MessageType }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	emitter, err := goka.NewEmitter(brokers,
		goka.Stream("{{ .TopicName }}"),
		codec,
		goka.WithEmitterHasher(kafkautil.MurmurHasher))

	if err != nil {
		return nil, errors.Wrap(err, "failed creating emitter")
	}

	return &{{ .Name }}_Emitter{
		emitter: runner.NewEmitter(emitter),
	}, nil
}

func (e *{{ .Name }}_Emitter) Watch(ctx context.Context) func() error {
	return e.emitter.Watch(ctx)
}

func (e *{{ .Name }}_Emitter) Emit(message *{{ .Name }}_Emitter_Message) error {
	return e.Emit(message.Key, message.Value)
}

func (e *{{ .Name }}_Emitter) EmitBulk(ctx context.Context, messages []*{{ .Name }}_Emitter_Message) error {
	return e.emitter.EmitBulk(ctx, messages)
}
`))
)

type emitterOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
}

func generateEmitter(writer io.Writer, emitter *emitterOptions) error {
	err := emitterTemplate.Execute(writer, emitter)
	if err != nil {
		return errors.Wrap(err, "failed to execute emitter template")
	}
	return nil
}

func buildEmitterOptions(pkg string, mod string, modelsPath string, emitter models.Emitter) (*emitterOptions, error) {
	options := &emitterOptions{
		Package: pkg,
	}

	var name strings.Builder
	nameFrags := strings.Split(emitter.Message, ".")
	for _, f := range nameFrags[1:] {
		name.WriteString(strcase.ToCamel(f))
		name.WriteString("_")
	}

	topic := emitter.Message
	if emitter.TopicDefinition.Topic != nil {
		topic = *emitter.TopicDefinition.Topic
	}

	options.TopicName = topic
	options.Name = strings.TrimRight(name.String(), "_")

	var mPkg strings.Builder
	for _, p := range nameFrags[:len(nameFrags)-1] {
		mPkg.WriteString("/")
		mPkg.WriteString(p)
	}

	imp := strings.TrimPrefix(mPkg.String(), "/")
	d := strings.Split(imp, "/")
	options.Import = fmt.Sprintf("%s \"%s%s/%s\"", d[len(d)-1], mod, modelsPath, imp)

	options.MessageType = nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])

	return options, nil
}
