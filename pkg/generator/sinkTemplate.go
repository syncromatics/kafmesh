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
	sinkTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
	"context"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/syncromatics/kafmesh/pkg/runner"
	"{{ .Import }}"
)

type {{ .Name }}_Sink interface {
	Flush() error
	Collect(ctx runner.MessageContext, key string, msg *{{ .MessageType }})
}

type impl_{{ .Name }}_Sink struct {
	{{ .Name }}_Sink sink
	codec goka.Codec
	group string
	topic string
	maxBufferSize int
	interval time.Duration
}

func (s *impl_{{ .Name }}_Sink) Codec() goka.Codec {
	return s.codec
}

func (s *impl_{{ .Name }}_Sink) Group() string {
	return s.group
}

func (s *impl_{{ .Name }}_Sink) Topic() string {
	return s.topic
}

func (s *impl_{{ .Name }}_Sink) MaxBufferSize() int {
	return s.maxBufferSize
}

func (s *impl_{{ .Name }}_Sink) Interval() time.Duration {
	return s.interval
}

func (s *impl_{{ .Name }}_Sink) Flush() error {
	return s.sink.Flush()
}

func (s *impl_{{ .Name }}_Sink) Collect(ctx runner.MessageContext, key string, msg interface{}) error {
	m, ok := msg.(*{{ .MessageType }})
	if !ok {
		return errors.Errorf("expecting message of type '*{{ .MessageType }}' got type '%t'", msg)
	}

	return s.sink.Collect(ctx, key, m)
}

func Register_{{ .Name }}_Sink(options runner.ServiceOptions, sink *{{ .Name }}_Sink, interval time.Duration, maxBufferSize int) (func(ctx context.Context) func(), error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("{{ .TopicName }}", &{{ .MessageType }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	d := &impl_{{ .Name }}_Sink{
		sink: sink,
		codec: codec,
		group: "{{ .GroupName }}",
		topic: "{{ .TopicName }}",
		maxBufferSize: maxBufferSize,
		interval: interval,
	}

	s, err := runner.NewSink(d, brokers)

	return func(ctx context.Context) func() error {
		return func() error {
			err := s.Run(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to run sink")
			}

			return nil
		}
	}, nil
}
`))
)

type sinkOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
	GroupName   string
}

func generateSink(writer io.Writer, sink *sinkOptions) error {
	err := sinkTemplate.Execute(writer, sink)
	if err != nil {
		return errors.Wrap(err, "failed to execute sink template")
	}
	return nil
}

func buildSinkOptions(pkg string, mod string, modelsPath string, sink models.Sink) (*sinkOptions, error) {
	options := &sinkOptions{
		Package: pkg,
	}

	var name strings.Builder
	nameFrags := strings.Split(sink.Message, ".")
	for _, f := range nameFrags[1:] {
		name.WriteString(strcase.ToCamel(f))
		name.WriteString("_")
	}

	topic := sink.Message
	if sink.TopicDefinition.Topic != nil {
		topic = *sink.TopicDefinition.Topic
	}

	options.TopicName = topic
	options.Name = strings.ReplaceAll(sink.Name, " ", "_")
	options.GroupName = fmt.Sprintf("%s-sink", strings.ToLower(options.Name))

	var mPkg strings.Builder
	for _, p := range nameFrags[:len(nameFrags)-1] {
		mPkg.WriteString("/")
		mPkg.WriteString(p)
	}

	imp := strings.TrimPrefix(mPkg.String(), "/")
	options.Import = fmt.Sprintf("%s%s/%s", mod, modelsPath, imp)

	options.MessageType = nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])

	return options, nil
}
