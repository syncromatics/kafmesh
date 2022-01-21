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
