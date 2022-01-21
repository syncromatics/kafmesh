package {{ .Package }}

import (
	"github.com/syncromatics/kafmesh/pkg/runner"
)

{{ range .Processors }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	processor := runner.ProcessorDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		Name: "{{ .Name }}",
		Description: "{{ .Description }}",
		GroupName: "{{ .GroupName }}",
		Inputs: []runner.InputDiscovery{
{{- range .Inputs }}
			{
				TopicDiscovery: runner.TopicDiscovery{
					Message: "{{ .Message }}",
					Topic: "{{ .Topic }}",
					Type: {{ .Type }},
				},
			},
{{- end }}
		},
		Joins: []runner.JoinDiscovery{
{{- range .Joins }}
			{
				TopicDiscovery: runner.TopicDiscovery{
					Message: "{{ .Message }}",
					Topic: "{{ .Topic }}",
					Type: {{ .Type }},
				},
			},
{{- end }}
		},
		Lookups: []runner.LookupDiscovery{
{{- range .Lookups }}
			{
				TopicDiscovery: runner.TopicDiscovery{
					Message: "{{ .Message }}",
					Topic: "{{ .Topic }}",
					Type: {{ .Type }},
				},
			},
{{- end }}
		},
		Outputs: []runner.OutputDiscovery{
{{- range .Outputs }}
			runner.OutputDiscovery{
				TopicDiscovery: runner.TopicDiscovery{
					Message: "{{ .Message }}",
					Topic: "{{ .Topic }}",
					Type: {{ .Type }},
				},
			},
{{- end }}
		},
{{- if .Persistence }}
		Persistence: &runner.PersistentDiscovery{
			TopicDiscovery: runner.TopicDiscovery{
				Message: "{{ .Persistence.Message }}",
				Topic: "{{ .Persistence.Topic }}",
				Type: {{ .Persistence.Type }},
			},
		},
{{- end }}
	}

	return service.RegisterProcessor(processor)
}

{{- end }}

{{ range .Sources }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	source := runner.SourceDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		TopicDiscovery: runner.TopicDiscovery{
			Message: "{{ .Source.Message }}",
			Topic: "{{ .Source.Topic }}",
			Type: {{ .Source.Type }},
		},
	}

	return service.RegisterSource(source)
}

{{- end }}

{{ range .Sinks }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	sink := runner.SinkDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		TopicDiscovery: runner.TopicDiscovery{
			Message: "{{ .Source.Message }}",
			Topic: "{{ .Source.Topic }}",
			Type: {{ .Source.Type }},
		},
		Name: "{{ .Name }}",
		Description: "{{ .Description }}",
	}

	return service.RegisterSink(sink)
}
{{- end }}

{{ range .Views }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	view := runner.ViewDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		TopicDiscovery: runner.TopicDiscovery{
			Message: "{{ .TopicDiscovery.Message }}",
			Topic: "{{ .TopicDiscovery.Topic }}",
			Type: {{ .TopicDiscovery.Type }},
		},
	}

	return service.RegisterView(view)
}
{{- end }}

{{ range .ViewSinks }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	sink := runner.ViewSinkDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		TopicDiscovery: runner.TopicDiscovery{
			Message: "{{ .Source.Message }}",
			Topic: "{{ .Source.Topic }}",
			Type: {{ .Source.Type }},
		},
		Name: "{{ .Name }}",
		Description: "{{ .Description }}",
	}

	return service.RegisterViewSink(sink)
}
{{- end }}

{{ range .ViewSources }}
func discover_{{ .MethodName }}(service *runner.Service) error {
	source := runner.ViewSourceDiscovery{
		ServiceDiscovery : runner.ServiceDiscovery {
			Name: "{{ .Service.Name}}",
			Description: "{{ .Service.Description }}",
		},
		ComponentDiscovery: runner.ComponentDiscovery{
			Name: "{{ .Component.Name}}",
			Description: "{{ .Component.Description }}",
		},
		TopicDiscovery: runner.TopicDiscovery{
			Message: "{{ .Source.Message }}",
			Topic: "{{ .Source.Topic }}",
			Type: {{ .Source.Type }},
		},
		Name: "{{ .Name }}",
		Description: "{{ .Description }}",
	}

	return service.RegisterViewSource(source)
}
{{- end }}
