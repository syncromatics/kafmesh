package generator

import (
	"fmt"
	"io"
	"path"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

var (
	serviceTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
{{- $sinkLength := len .Sinks -}} {{- $syncLength := len .ViewSources -}} {{- if or (ne $sinkLength 0) (ne $syncLength 0) }}
	"time"
{{- end }}

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
{{ range .Imports }}
	{{ printf "%q" . }}
{{- end }}
)
{{ range .Processors }}
func Register_{{ .ExportName }}(service *runner.Service, processor {{ .Package }}.{{ .Name }}) error {
	r, err := {{ .Package }}.Register_{{ .Name }}(service, processor)
	if err != nil {
		return errors.Wrap(err, "failed to register processor")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	err = discover_{{ .ExportName }}(service)
	if err != nil {
		return errors.Wrap(err, "failed to register with discovery")
	}

	return nil
}
{{ end -}}

{{ range .Sources }}
func New_{{ .ExportName }}_Source(service *runner.Service) ({{ .Package }}.{{ .Name }}_Source, error) {
	e, r, err := {{ .Package }}.New_{{ .Name }}_Source(service)
	if err != nil {
		return nil, err
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register runner with service")
	}

	err = discover_{{ .ExportName }}_Source(service)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register with discovery")
	}

	return e, nil
}
{{ end -}}

{{ range .Views }}
func New_{{ .ExportName }}_View(service *runner.Service) ({{ .Package }}.{{ .Name }}_View, error) {
	v, r, err := {{ .Package }}.New_{{ .Name }}_View(service.Options())
	if err != nil {
		return nil, err
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register runner with service")
	}

	err = discover_{{ .ExportName }}_View(service)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register with discovery")
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

	err = discover_{{ .ExportName }}_Sink(service)
	if err != nil {
		return errors.Wrap(err, "failed to register with discovery")
	}

	return nil
}
{{ end -}}

{{ range .ViewSources }}
func Register_{{ .ExportName }}_ViewSource(service *runner.Service, viewSource {{ .Package }}.{{ .Name }}_ViewSource, updateInterval time.Duration, syncTimeout time.Duration) error {
	r, err := {{ .Package }}.Register_{{ .Name }}_ViewSource(service.Options(), viewSource, updateInterval, syncTimeout)
	if err != nil {
		return errors.Wrap(err, "failed to register viewSource")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	err = discover_{{ .ExportName }}_ViewSource(service)
	if err != nil {
		return errors.Wrap(err, "failed to register with discovery")
	}

	return nil
}
{{ end -}}

{{ range .ViewSinks }}
func Register_{{ .ExportName }}_ViewSink(service *runner.Service, viewSink {{ .Package }}.{{ .Name }}_ViewSink, updateInterval time.Duration, syncTimeout time.Duration) error {
	r, err := {{ .Package }}.Register_{{ .Name }}_ViewSink(service.Options(), viewSink, updateInterval, syncTimeout)
	if err != nil {
		return errors.Wrap(err, "failed to register viewSink")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	err = discover_{{ .ExportName }}_ViewSink(service)
	if err != nil {
		return errors.Wrap(err, "failed to register with discovery")
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

type serviceSource struct {
	Name       string
	ExportName string
	Package    string
}

type serviceView struct {
	Name       string
	ExportName string
	Package    string
}

type serviceSink struct {
	Name       string
	ExportName string
	Package    string
}

type serviceViewSource struct {
	Name       string
	ExportName string
	Package    string
}

type serviceViewSink struct {
	Name       string
	ExportName string
	Package    string
}

type generateServiceOptions struct {
	Package     string
	Imports     []string
	Processors  []serviceProcessor
	Sources     []serviceSource
	Views       []serviceView
	Sinks       []serviceSink
	ViewSources []serviceViewSource
	ViewSinks   []serviceViewSink
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

		for _, e := range c.Sources {
			proc := serviceSource{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), e.ToSafeMessageTypeName()),
				Name:       e.ToSafeMessageTypeName(),
			}
			options.Sources = append(options.Sources, proc)
		}

		for _, v := range c.Views {
			proc := serviceView{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), v.ToSafeMessageTypeName()),
				Name:       v.ToSafeMessageTypeName(),
			}
			options.Views = append(options.Views, proc)
		}

		for _, s := range c.Sinks {
			proc := serviceSink{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), s.ToSafeName()),
				Name:       s.ToSafeName(),
			}
			options.Sinks = append(options.Sinks, proc)
		}

		for _, s := range c.ViewSources {
			proc := serviceViewSource{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), s.ToSafeName()),
				Name:       fmt.Sprintf("%s", s.ToSafeName()),
			}
			options.ViewSources = append(options.ViewSources, proc)
		}

		for _, s := range c.ViewSinks {
			proc := serviceViewSink{
				Package:    c.Name,
				ExportName: fmt.Sprintf("%s_%s", c.ToSafeName(), s.ToSafeName()),
				Name:       fmt.Sprintf("%s", s.ToSafeName()),
			}
			options.ViewSinks = append(options.ViewSinks, proc)
		}
	}

	return options, nil
}
