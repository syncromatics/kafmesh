package generator

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
)

var (
	processorTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

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

	m := v.(*{{- $t.MessageType -}})
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

	m := v.(*{{- $t.MessageType -}})
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
	c{{ .Index }}, err := protoWrapper.Codec("{{ .Topic }}", &{{ .Message }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}
{{ end }}
	edges := []goka.Edge{
{{- range .Edges -}}
{{ $e := . }}
{{- with (eq .Type "input" ) }}
		goka.Input(goka.Stream("{{ $e.Topic }}"), c{{ $e.Codec }}, func(ctx goka.Context, m interface{}) {
			msg := m.(*{{ $e.Message }})

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
`))
)

type edge struct {
	Type        string
	Topic       string
	Message     string
	MessageType string
	Codec       int
	Func        string
}

type processorInterface struct {
	Name    string
	Methods []interfaceMethod
}

type interfaceMethod struct {
	Name string
	Args string
}

type contextMethod struct {
	interfaceMethod
	Type            string
	Topic           string
	MessageType     string
	MessageTypeName string
}

type processorContext struct {
	Name    string
	Methods []contextMethod
}

type codec struct {
	Index   int
	Message string
	Topic   string
}

type processorOptions struct {
	Package       string
	Component     string
	ProcessorName string
	Context       processorContext
	Interface     processorInterface
	Imports       []string
	Group         string
	Edges         []edge
	Codecs        []codec
	Processor     models.Processor
}

func generateProcessor(writer io.Writer, processor *processorOptions) error {
	err := processorTemplate.Execute(writer, processor)
	if err != nil {
		return errors.Wrap(err, "failed to execute processor template")
	}
	return nil
}

func buildProcessorOptions(pkg string, mod string, modelsPath string, service *models.Service, component *models.Component, processor models.Processor) (*processorOptions, error) {
	imports := map[string]int{}
	importIndex := 0

	codecs := map[string]codec{}
	codecIndex := 0

	options := processorOptions{
		Package:       pkg,
		Component:     component.Name,
		ProcessorName: processor.Name,
		Imports:       []string{},
		Group:         processor.GroupName(service, component),
		Edges:         []edge{},
		Codecs:        []codec{},
		Processor:     models.Processor{},
	}

	options.Context = processorContext{
		Name:    processor.ToSafeName(),
		Methods: []contextMethod{},
	}

	intr := processorInterface{
		Name:    processor.ToSafeName(),
		Methods: []interfaceMethod{},
	}

	for _, input := range processor.Inputs {
		var name strings.Builder
		name.WriteString("Handle")
		nameFrags := strings.Split(input.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		var mPkg strings.Builder
		for _, p := range nameFrags[:len(nameFrags)-1] {
			mPkg.WriteString("/")
			mPkg.WriteString(p)
		}

		modulePackage := input.ToPackage(service)

		i, ok := imports[modulePackage]
		if !ok {
			imports[modulePackage] = importIndex
			i = importIndex

			importIndex++
		}

		var args strings.Builder
		args.WriteString(fmt.Sprintf("ctx %s_ProcessorContext", options.Context.Name))
		message := nameFrags[len(nameFrags)-1]
		args.WriteString(fmt.Sprintf(", message *m%d.%s", i, strcase.ToCamel(message)))

		method := interfaceMethod{
			Name: fmt.Sprintf("Handle%s", input.ToSafeMessageTypeName()),
			Args: args.String(),
		}
		intr.Methods = append(intr.Methods, method)

		topic := input.ToTopicName(service)
		c, ok := codecs[topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:        "input",
			Topic:       topic,
			Message:     fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			MessageType: input.Message,
			Codec:       c.Index,
			Func:        method.Name,
		})
	}
	options.Interface = intr

	for _, lookup := range processor.Lookups {
		var name strings.Builder
		name.WriteString("Lookup_")

		nameFrags := strings.Split(lookup.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		var mPkg strings.Builder
		for _, p := range nameFrags[:len(nameFrags)-1] {
			mPkg.WriteString("/")
			mPkg.WriteString(p)
		}

		modulePackage := lookup.ToPackage(service)

		i, ok := imports[modulePackage]
		if !ok {
			imports[modulePackage] = importIndex
			i = importIndex

			importIndex++
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]
		args.WriteString(fmt.Sprintf("key string) *m%d.%s", i, strcase.ToCamel(message)))

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Lookup_%s", lookup.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "lookup",
			MessageType:     fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			MessageTypeName: lookup.Message,
		}

		m.Topic = lookup.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:  "lookup",
			Codec: c.Index,
			Topic: m.Topic,
		})
	}

	for _, join := range processor.Joins {
		var name strings.Builder
		name.WriteString("Join_")
		nameFrags := strings.Split(join.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		var mPkg strings.Builder
		for _, p := range nameFrags[:len(nameFrags)-1] {
			mPkg.WriteString("/")
			mPkg.WriteString(p)
		}

		modulePackage := join.ToPackage(service)

		i, ok := imports[modulePackage]
		if !ok {
			imports[modulePackage] = importIndex
			i = importIndex

			importIndex++
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]
		args.WriteString(fmt.Sprintf(") *m%d.%s", i, strcase.ToCamel(message)))

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Join_%s", join.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "join",
			MessageType:     fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			MessageTypeName: join.Message,
		}

		m.Topic = join.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:  "join",
			Codec: c.Index,
			Topic: m.Topic,
		})
	}

	for _, output := range processor.Outputs {
		var name strings.Builder
		name.WriteString("Output_")
		nameFrags := strings.Split(output.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		var mPkg strings.Builder
		for _, p := range nameFrags[:len(nameFrags)-1] {
			mPkg.WriteString("/")
			mPkg.WriteString(p)
		}

		modulePackage := output.ToPackage(service)

		i, ok := imports[modulePackage]
		if !ok {
			imports[modulePackage] = importIndex
			i = importIndex

			importIndex++
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]
		args.WriteString(fmt.Sprintf("key string, message *m%d.%s)", i, strcase.ToCamel(message)))

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Output_%s", output.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "output",
			MessageTypeName: output.Message,
			Topic:           output.ToTopicName(service),
		}

		m.Topic = output.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:  "output",
			Codec: c.Index,
			Topic: m.Topic,
		})
	}

	if processor.Persistence != nil {
		nameFrags := strings.Split(processor.Persistence.Message, ".")

		var mPkg strings.Builder
		for _, p := range nameFrags[:len(nameFrags)-1] {
			mPkg.WriteString("/")
			mPkg.WriteString(p)
		}

		modulePackage := processor.Persistence.ToPackage(service)

		i, ok := imports[modulePackage]
		if !ok {
			imports[modulePackage] = importIndex
			i = importIndex

			importIndex++
		}
		message := nameFrags[len(nameFrags)-1]

		options.Context.Methods = append(options.Context.Methods, contextMethod{
			interfaceMethod: interfaceMethod{
				Name: "SaveState",
				Args: fmt.Sprintf("state *m%d.%s)", i, strcase.ToCamel(message)),
			},
			Type:            "save",
			MessageTypeName: processor.Persistence.Message,
			Topic:           options.Group + "-table",
		})

		options.Context.Methods = append(options.Context.Methods, contextMethod{
			interfaceMethod: interfaceMethod{
				Name: "State",
				Args: fmt.Sprintf(") *m%d.%s", i, strcase.ToCamel(message)),
			},
			Type:            "state",
			MessageType:     fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			MessageTypeName: processor.Persistence.Message,
			Topic:           options.Group + "-table",
		})

		c, ok := codecs[options.Group+"-table"]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   options.Group + "-table",
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[options.Group+"-table"] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:  "state",
			Codec: c.Index,
		})
	}

	for _, c := range codecs {
		options.Codecs = append(options.Codecs, c)
	}

	sort.SliceStable(options.Codecs, func(i, j int) bool {
		return options.Codecs[i].Index < options.Codecs[j].Index
	})

	for k, v := range imports {
		imp := fmt.Sprintf("m%d \"%s\"", v, k)
		options.Imports = append(options.Imports, imp)
	}

	sort.Strings(options.Imports)

	options.Processor = processor

	return &options, nil
}
