package {{ .Package }}

import (
	"context"
	"time"

	"github.com/lovoo/goka"
	"github.com/pkg/errors"

	"github.com/syncromatics/kafmesh/pkg/runner"

	{{ .Import }}
)

{{ $t := . -}}
type {{ .Name }}_Sink interface {
	Flush() error
	{{- with (eq .Type "protobuf") }}
	Collect(ctx runner.MessageContext, key string, msg *{{ $t.MessageType }}) error
	{{- end -}}
	{{- with (eq .Type "raw") }}
	Collect(ctx runner.MessageContext, key string, msg []byte) error
	{{- end }}
}

type impl_{{ .Name }}_Sink struct {
	sink {{ .Name }}_Sink
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
	{{- with (eq .Type "protobuf") }}
	m, ok := msg.(*{{ $t.MessageType }})
	{{- end -}}
	{{- with (eq .Type "raw") }}
	m, ok := msg.([]byte)
	{{- end }}
	
	if !ok {
		return errors.Errorf("expecting message of type '*{{ .MessageType }}' got type '%t'", msg)
	}

	return s.sink.Collect(ctx, key, m)
}

func Register_{{ .Name }}_Sink(options runner.ServiceOptions, sink {{ .Name }}_Sink, interval time.Duration, maxBufferSize int) (func(ctx context.Context) func() error, error) {
	brokers := options.Brokers

	{{- with (eq .Type "protobuf") }}
	protoWrapper := options.ProtoWrapper
	codec, err := protoWrapper.Codec("{{ $t.TopicName }}", &{{ $t.MessageType }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	codec := &gokaCodecs.Bytes{}
	{{- end }}

	d := &impl_{{ .Name }}_Sink{
		sink:          sink,
		codec:         codec,
		group:         "{{ .GroupName }}",
		topic:         "{{ .TopicName }}",
		maxBufferSize: maxBufferSize,
		interval:      interval,
	}

	s := runner.NewSinkRunner(d, brokers)

	return func(ctx context.Context) func() error {
		return s.Run(ctx)
	}, nil
}
