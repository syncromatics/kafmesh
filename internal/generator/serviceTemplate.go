package generator

import (
	"fmt"
	"io"
	"path"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

var (
	serviceTemplate = template.Must(template.New("").Parse(templates.Service))
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
