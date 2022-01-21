package {{ .Package }}

import (
	"context"
	
	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
	"golang.org/x/sync/errgroup"

	{{ .Import }}
)

{{ $t := . -}}
type {{ .Name }}_Source interface {
	Emit(message {{ .Name }}_Source_Message) error
	EmitBulk(ctx context.Context, messages []{{ .Name }}_Source_Message) error
	Delete(key string) error
}

type {{ .Name }}_Source_impl struct {
	context.Context
	emitter *runner.Emitter
	metrics *runner.Metrics
}

type {{ .Name }}_Source_Message struct {
	Key string
	{{- with (eq .Type "protobuf") }}
	Value *{{ $t.MessageType }}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	Value []byte
	{{- end }}
}

type impl_{{ .Name }}_Source_Message struct {
	msg {{ .Name }}_Source_Message
}

func (m *impl_{{ .Name }}_Source_Message) Key() string {
	return m.msg.Key
}

func (m *impl_{{ .Name }}_Source_Message) Value() interface{} {
	return m.msg.Value
}

func New_{{ .Name }}_Source(service *runner.Service) (*{{ .Name }}_Source_impl, func(context.Context) func() error, error) {
	options := service.Options()
	brokers := options.Brokers

	{{- with (eq .Type "protobuf") }}
	protoWrapper := options.ProtoWrapper
	codec, err := protoWrapper.Codec("{{ $t.TopicName }}", &{{ $t.MessageType }}{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create codec")
	}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	codec := &gokaCodecs.Bytes{}
	{{- end }}

	emitter, err := goka.NewEmitter(brokers,
		goka.Stream("{{ .TopicName }}"),
		codec,
		goka.WithEmitterHasher(kafkautil.MurmurHasher))

	if err != nil {
		return nil, nil, errors.Wrap(err, "failed creating source")
	}

	emitterCtx, emitterCancel := context.WithCancel(context.Background())
	e := &{{ .Name }}_Source_impl{
		emitterCtx,
		runner.NewEmitter(emitter),
		service.Metrics,
	}

	return e, func(outerCtx context.Context) func() error {
		return func() error {
			cancelableCtx, cancel := context.WithCancel(outerCtx)
			defer cancel()
			grp, ctx := errgroup.WithContext(cancelableCtx)

			grp.Go(func() error {
				select {
				case <-ctx.Done():
					emitterCancel()
					return nil
				}
			})
			grp.Go(e.emitter.Watch(ctx))

			select {
			case <- ctx.Done():
				err := grp.Wait()
				return err
			}
		}
	}, nil
}

func (e *{{ .Name }}_Source_impl) Emit(message {{ .Name }}_Source_Message) error {
	err := e.emitter.Emit(message.Key, message.Value)
	if err != nil {
		e.metrics.SourceError("{{ .ServiceName }}", "{{ .ComponentName }}", "{{ .TopicName }}")
		return err
	}

	e.metrics.SourceHit("{{ .ServiceName }}", "{{ .ComponentName }}", "{{ .TopicName }}", 1)
	return nil
}

func (e *{{ .Name }}_Source_impl) EmitBulk(ctx context.Context, messages []{{ .Name }}_Source_Message) error {
	b := []runner.EmitMessage{}
	for _, m := range messages {
		b = append(b, &impl_{{ .Name }}_Source_Message{msg: m})
	}
	err := e.emitter.EmitBulk(ctx, b)
	if err != nil {
		e.metrics.SourceError("{{ .ServiceName }}", "{{ .ComponentName }}", "{{ .TopicName }}")
		return err
	}

	e.metrics.SourceHit("{{ .ServiceName }}", "{{ .ComponentName }}", "{{ .TopicName }}", len(b))
	return nil
}

func (e *{{ .Name }}_Source_impl) Delete(key string) error {
	return e.emitter.Emit(key, nil)
}
