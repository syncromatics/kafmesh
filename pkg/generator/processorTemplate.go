package generator

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"github.com/syncromatics/kafmesh/pkg/models"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
)

var (
	processorTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

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
{{ range .Imports }}
	{{ . }}
{{- end }}
)

{{ with .Context -}}
type {{ .Name }}_ProcessorContext interface {
	Key() string
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
	ctx goka.Context
}

func new_{{ .Name }}_ProcessorContext_Impl(ctx goka.Context) *{{ .Name }}_ProcessorContext_Impl {
	return &{{ .Name }}_ProcessorContext_Impl{ctx}
}
{{$c := .Name}}
func (c *{{$c}}_ProcessorContext_Impl) Key() string {
	return c.ctx.Key()
}
{{ range .Methods }}
func (c *{{$c}}_ProcessorContext_Impl) {{.Name}}({{ .Args }} {
{{- $t := . -}}
{{- with (eq .Type "lookup" ) }}
	v := c.ctx.Lookup("{{- $t.Topic -}}", key)
	return v.(*{{- $t.MessageType -}})
{{- end -}}
{{- with (eq .Type "join" ) }}
	v := c.ctx.Join("{{- $t.Topic -}}")
	return v.(*{{- $t.MessageType -}})
{{- end -}}
{{- with (eq .Type "output" ) }}
	c.ctx.Emit("{{- $t.Topic -}}", key, message)
{{- end -}}
{{- with (eq .Type "save") }}
	c.ctx.SetValue(state)
{{- end -}}
{{- with (eq .Type "state") }}
	v := c.ctx.Value()
	t := v.(*{{- $t.MessageType -}})
	if t == nil {
		t = &{{- $t.MessageType -}}{}
	}
	return t
{{- end }}
}
{{ end}}
{{- end}}
{{ $c := .Context -}}
{{ with .Interface -}}
func Register_{{ .Name }}_Processor(options runner.ServiceOptions, service {{ .Name }}_Processor) (func(context.Context) func() error, error) {
{{- end }}
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	config := kafka.NewConfig()
	config.Consumer.Offsets.Initial = kafka.OffsetOldest

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	builder := storage.BuilderWithOptions("/tmp/storage", opts)
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
			w := new_{{ $c.Name }}_ProcessorContext_Impl(ctx)
			err := service.{{ $e.Func }}(w, msg)
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
		goka.WithConsumerBuilder(kafka.ConsumerBuilderWithConfig(config)),
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
	Type    string
	Topic   string
	Message string
	Codec   int
	Func    string
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
	Type        string
	Topic       string
	MessageType string
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
	Package   string
	Context   processorContext
	Interface processorInterface
	Imports   []string
	Group     string
	Edges     []edge
	Codecs    []codec
}

func generateProcessor(writer io.Writer, processor *processorOptions) error {
	err := processorTemplate.Execute(writer, processor)
	if err != nil {
		return errors.Wrap(err, "failed to execute processor template")
	}
	return nil
}

func buildProcessorOptions(pkg string, mod string, modelsPath string, processor models.Processor) (*processorOptions, error) {
	imports := map[string]int{}
	importIndex := 0

	codecs := map[string]codec{}
	codecIndex := 0

	options := processorOptions{
		Package: pkg,
		Imports: []string{},
		Group:   processor.GroupName,
		Edges:   []edge{},
		Codecs:  []codec{},
	}

	var iname strings.Builder
	for _, s := range strings.Split(processor.GroupName, ".") {
		iname.WriteString(strcase.ToCamel(s))
	}

	options.Context = processorContext{
		Name:    iname.String(),
		Methods: []contextMethod{},
	}

	intr := processorInterface{
		Name:    iname.String(),
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

		modulePackage := strings.TrimPrefix(mPkg.String(), "/")

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

		topic := input.Message
		if input.TopicDefinition.Topic != nil {
			topic = *input.TopicDefinition.Topic
		}

		c, ok := codecs[message]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[message] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:    "input",
			Topic:   topic,
			Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			Codec:   c.Index,
			Func:    method.Name,
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

		modulePackage := strings.TrimPrefix(mPkg.String(), "/")

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
				Name: name.String(),
				Args: args.String(),
			},
			Type:        "lookup",
			MessageType: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
		}

		if lookup.TopicDefinition.Topic != nil {
			m.Topic = *lookup.TopicDefinition.Topic
		} else {
			m.Topic = lookup.Message
		}
		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[message]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[message] = c
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

		modulePackage := strings.TrimPrefix(mPkg.String(), "/")

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
				Name: name.String(),
				Args: args.String(),
			},
			Type:        "join",
			MessageType: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
		}

		if join.TopicDefinition.Topic != nil {
			m.Topic = *join.TopicDefinition.Topic
		} else {
			m.Topic = join.Message
		}

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[message]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[message] = c
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

		modulePackage := strings.TrimPrefix(mPkg.String(), "/")

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
				Name: name.String(),
				Args: args.String(),
			},
			Type: "output",
		}
		if output.TopicDefinition.Topic != nil {
			m.Topic = *output.TopicDefinition.Topic
		} else {
			m.Topic = output.Message
		}

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[message]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[message] = c
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

		modulePackage := strings.TrimPrefix(mPkg.String(), "/")

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
			Type: "save",
		})

		options.Context.Methods = append(options.Context.Methods, contextMethod{
			interfaceMethod: interfaceMethod{
				Name: "State",
				Args: fmt.Sprintf(") *m%d.%s", i, strcase.ToCamel(message)),
			},
			Type:        "state",
			MessageType: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
		})

		c, ok := codecs[message]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   options.Group + "-table",
				Message: fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message)),
			}
			codecs[message] = c
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
		imp := fmt.Sprintf("m%d \"%s%s/%s\"", v, mod, modelsPath, k)
		options.Imports = append(options.Imports, imp)
	}

	sort.Strings(options.Imports)

	return &options, nil
}
