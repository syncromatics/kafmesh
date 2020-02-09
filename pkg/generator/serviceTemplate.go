package generator

import (
	"fmt"
	"io"
	"path"
	"strings"
	"text/template"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/models"
)

var (
	serviceTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
	"time"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
{{ range .Imports }}
	{{ printf "%q" . }}
{{- end }}
)
{{ range .Processors }}
func Register_{{ .ExportName }}(service *runner.Service, processor {{ .Package }}.{{ .Name }}) error {
	r, err := {{ .Package }}.Register_{{ .Name }}(service.Options(), processor)
	if err != nil {
		return errors.Wrap(err, "failed to register processor")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	return nil
}
{{ end -}}

{{ range .Emitters }}
func New_{{ .Name }}_Emitter(service *runner.Service) ({{ .Package }}.{{ .Name }}_Emitter, error) {
	e, err := {{ .Package }}.New_{{ .Name }}_Emitter(service.Options())
	if err != nil {
		return nil, err
	}

	err = service.RegisterRunner(e.Watch)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register runner with service")
	}

	return e, nil
}
{{ end -}}

{{ range .Views }}
func New_{{ .Name }}_View(service *runner.Service) ({{ .Package }}.{{ .Name }}_View, error) {
	v, err := {{ .Package }}.New_{{ .Name }}_View(service.Options())
	if err != nil {
		return nil, err
	}

	err = service.RegisterRunner(v.Watch)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register runner with service")
	}

	return v, nil
}
{{ end -}}

{{ range .Sinks }}
func Register_{{ .Name }}_Sink(service *runner.Service, sink {{ .Package }}.{{ .Name }}_Sink, interval time.Duration, maxBufferSize int) error {
	r, err := {{ .Package }}.Register_{{ .Name }}_Sink(service.Options(), sink, interval, maxBufferSize)
	if err != nil {
		return errors.Wrap(err, "failed to register sink")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	return nil
}
{{ end -}}

{{ range .Synchronizers }}
func Register_{{ .ExportName }}_Synchronizer(service *runner.Service, synchronizer {{ .Package }}.{{ .Name }}_Synchronizer, updateInterval time.Duration) error {
	r, err := {{ .Package }}.Register_{{ .Name }}_Synchronizer(service.Options(), synchronizer, updateInterval)
	if err != nil {
		return errors.Wrap(err, "failed to register sychronizer")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	return nil
}
{{ end -}}
`))
)

type serviceProcessor struct {
	Name       string
	ExportName string
	Package    string
}

type serviceEmitter struct {
	Name    string
	Package string
}

type serviceView struct {
	Name    string
	Package string
}

type serviceSink struct {
	Name    string
	Package string
}

type serviceSynchronizer struct {
	Name       string
	ExportName string
	Package    string
}

type generateServiceOptions struct {
	Package       string
	Imports       []string
	Processors    []serviceProcessor
	Emitters      []serviceEmitter
	Views         []serviceView
	Sinks         []serviceSink
	Synchronizers []serviceSynchronizer
}

func generateService(writer io.Writer, options generateServiceOptions) error {
	err := serviceTemplate.Execute(writer, options)
	if err != nil {
		return errors.Wrap(err, "failed to execute service template")
	}

	return nil
}

func buildServiceOptions(service *models.Service, components []*models.Component, mod string) (generateServiceOptions, error) {
	options := generateServiceOptions{
		Package: service.Output.Package,
	}

	p := path.Join(mod, service.Output.Path)

	for _, c := range components {
		options.Imports = append(options.Imports, path.Join(p, c.Name))

		for _, p := range c.Processors {
			proc := serviceProcessor{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s_Processor", c.ToSafeName(), p.ToSafeName()),
				Name:       fmt.Sprintf("%s_Processor", p.ToSafeName()),
			}
			options.Processors = append(options.Processors, proc)
		}

		for _, e := range c.Emitters {
			var name strings.Builder
			nameFrags := strings.Split(e.Message, ".")
			for _, f := range nameFrags[1:] {
				name.WriteString(strcase.ToCamel(f))
			}

			proc := serviceEmitter{
				Package: c.Name,
				Name:    strings.TrimRight(name.String(), "_"),
			}
			options.Emitters = append(options.Emitters, proc)
		}

		for _, v := range c.Views {
			var name strings.Builder
			nameFrags := strings.Split(v.Message, ".")
			for _, f := range nameFrags[1:] {
				name.WriteString(strcase.ToCamel(f))
			}

			proc := serviceView{
				Package: c.Name,
				Name:    strings.TrimRight(name.String(), "_"),
			}
			options.Views = append(options.Views, proc)
		}

		for _, s := range c.Sinks {
			var name strings.Builder
			nameFrags := strings.Split(s.Name, " ")
			for _, f := range nameFrags {
				name.WriteString(strcase.ToCamel(f))
			}

			proc := serviceSink{
				Package: c.Name,
				Name:    name.String(),
			}
			options.Sinks = append(options.Sinks, proc)
		}

		for _, s := range c.Synchronizers {
			var name strings.Builder
			nameFrags := strings.Split(s.Message, ".")
			for _, f := range nameFrags[1:] {
				name.WriteString(strcase.ToCamel(f))
			}

			proc := serviceSynchronizer{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), s.ToSafeName()),
				Name:       fmt.Sprintf("%s", s.ToSafeName()),
			}
			options.Synchronizers = append(options.Synchronizers, proc)
		}
	}

	return options, nil
}
