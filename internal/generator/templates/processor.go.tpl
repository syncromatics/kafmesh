package {{ .Package }}

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/syncromatics/kafmesh/pkg/runner"
{{ range .Imports }}
	{{ . }}
{{- end }}
)

{{ with .Context -}}
type {{ .Name }}_ProcessorContext interface {
	Key() string
	Timestamp() time.Time
	{{- range .Methods }}
	{{.Name}}({{ .Args }}
{{- end}}
}
{{- end }}

{{ with .Interface -}}
type {{ .Name }}_Processor interface {
	{{- range .Methods }}
	{{.Name}}({{ .Args }}) error
{{- end}}
}
{{- end}}
{{ $impl := "" }}
{{ with .Context -}}
type {{ .Name }}_ProcessorContext_Impl struct {
	ctx              goka.Context
	processorContext *runner.ProcessorContext
}

func new_{{ .Name }}_ProcessorContext_Impl(ctx goka.Context, pc *runner.ProcessorContext) *{{ .Name }}_ProcessorContext_Impl {
	return &{{ .Name }}_ProcessorContext_Impl{ctx, pc}
}
{{$c := .Name}}
func (c *{{$c}}_ProcessorContext_Impl) Key() string {
	return c.ctx.Key()
}

func (c *{{$c}}_ProcessorContext_Impl) Timestamp() time.Time {
	return c.ctx.Timestamp()
}
{{ range .Methods }}
func (c *{{$c}}_ProcessorContext_Impl) {{.Name}}({{ .Args }} {
{{- $t := . -}}
{{- with (eq .Type "lookup" ) }}
	v := c.ctx.Lookup("{{- $t.Topic -}}", key)
	if v == nil {
		c.processorContext.Lookup("{{$t.Topic}}", "{{$t.MessageTypeName}}", key, "")
		return nil
	}

	{{ with (eq $t.RequiresPointer true) -}}
	m := v.(*{{ $t.MessageType }})
	{{- end -}}
	{{- with (eq $t.RequiresPointer false) -}}
	m := v.({{ $t.MessageType }})
	{{- end }}

	value, _ := json.Marshal(m)
	c.processorContext.Lookup("{{ $t.Topic }}", "{{$t.MessageTypeName}}", key, string(value))

	return m
{{- end -}}
{{- with (eq .Type "join" ) }}
	v := c.ctx.Join("{{- $t.Topic -}}")
	if v == nil {
		c.processorContext.Join("{{$t.Topic}}", "{{$t.MessageTypeName}}", "")
		return nil
	}

	{{ with (eq $t.RequiresPointer true) -}}
	m := v.(*{{ $t.MessageType }})
	{{- end -}}
	{{- with (eq $t.RequiresPointer false) -}}
	m := v.({{ $t.MessageType }})
	{{- end }}

	value, _ := json.Marshal(m)
	c.processorContext.Join("{{ $t.Topic }}", "{{$t.MessageTypeName}}", string(value))

	return m
{{- end -}}
{{- with (eq .Type "output" ) }}
	value, _ := json.Marshal(message)
	c.processorContext.Output("{{ $t.Topic }}", "{{$t.MessageTypeName}}", key, string(value))
	c.ctx.Emit("{{- $t.Topic -}}", key, message)
{{- end -}}
{{- with (eq .Type "save") }}
	value, _ := json.Marshal(state)
	c.processorContext.SetState("{{ $t.Topic }}", "{{$t.MessageTypeName}}", string(value))

	c.ctx.SetValue(state)
{{- end -}}
{{- with (eq .Type "state") }}
	v := c.ctx.Value()
	var m *{{- $t.MessageType }}
	if v == nil {
		m = &{{- $t.MessageType -}}{}
	} else {
		m = v.(*{{- $t.MessageType -}})
	}

	value, _ := json.Marshal(m)
	c.processorContext.GetState("{{ $t.Topic }}", "{{$t.MessageTypeName}}", string(value))

	return m
{{- end }}
}
{{ end}}
{{- end}}
{{ $c := .Context -}}
{{- $componentName := .Component -}}
{{- $processorName := .ProcessorName -}}
{{ with .Interface -}}
func Register_{{ .Name }}_Processor(service *runner.Service, impl {{ .Name }}_Processor) (func(context.Context) func() error, error) {
{{- end }}
	options := service.Options()
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "processor", "{{ .Group }}")

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create processor db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)
{{ range .Codecs }}
	{{- $cd := . -}}
	{{- with (eq .Type "protobuf") }}
	c{{ $cd.Index }}, err := protoWrapper.Codec("{{ $cd.Topic }}", &{{ $cd.Message }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	c{{ $cd.Index }} := &gokaCodecs.Bytes{}
	{{- end -}}
{{ end }}

	edges := []goka.Edge{
{{- range .Edges -}}
{{ $e := . }}
{{- with (eq .Type "input" ) }}
		goka.Input(goka.Stream("{{ $e.Topic }}"), c{{ $e.Codec }}, func(ctx goka.Context, m interface{}) {
			{{- $mt := . -}}
			{{- with (eq $e.RequiresPointer true) }}
			msg := m.(*{{ $e.Message }})
			{{- end }}
			{{- with (eq $e.RequiresPointer false) }}
			msg := m.({{ $e.Message }})
			{{- end }}

			pc := service.ProcessorContext(ctx.Context(), "{{$componentName}}", "{{$processorName}}", ctx.Key())
			defer pc.Finish()

			v, err := json.Marshal(msg)
			if err != nil {
				ctx.Fail(err)
			}
			pc.Input("{{ $e.Topic }}", "{{ $e.MessageType}}", string(v))

			w := new_{{ $c.Name }}_ProcessorContext_Impl(ctx, pc)
			err = impl.{{ $e.Func }}(w, msg)
			if err != nil {
				ctx.Fail(err)
			}
		}),
{{- end -}}
{{- with (eq .Type "lookup" ) }}
		goka.Lookup(goka.Table("{{ $e.Topic }}"), c{{ $e.Codec }}),
{{- end -}}
{{- with (eq .Type "join" ) }}
		goka.Join(goka.Table("{{ $e.Topic }}"), c{{ $e.Codec }}),
{{- end -}}
{{- with (eq .Type "output" ) }}
		goka.Output(goka.Stream("{{ $e.Topic }}"), c{{ $e.Codec }}),
{{- end -}}
{{- with (eq .Type "state" ) }}
		goka.Persist(c{{ $e.Codec }}),
{{- end -}}
{{ end }}
	}
	group := goka.DefineGroup(goka.Group("{{ .Group }}"), edges...)

	processor, err := goka.NewProcessor(brokers,
		group,
		goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(config)),
		goka.WithStorageBuilder(builder),
		goka.WithHasher(kafkautil.MurmurHasher))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create goka processor")
	}

	return func(ctx context.Context) func() error {
		return func() error {
			err := processor.Run(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to run goka processor")
			}

			return nil
		}
	}, nil
}
