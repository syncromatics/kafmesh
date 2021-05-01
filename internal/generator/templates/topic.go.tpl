package {{ .Package }}

import (
	"context"
	"time"

	"github.com/syncromatics/kafmesh/pkg/runner"
)

var (
	topics = []runner.Topic{
		{{- range .Topics }}
		runner.Topic {
			Name:       "{{ .Name }}",
			Partitions: {{ .Partitions}},
			Replicas:   {{ .Replicas }},
			Compact:    {{ .Compact }},
			Retention:  {{ .Retention.Milliseconds }} * time.Millisecond,
			Segment:    {{ .Segment.Milliseconds }} * time.Millisecond,
			Create:     {{ .Create }},
		},
		{{- end }}
	}
)

func ConfigureTopics(ctx context.Context, brokers []string) error {
	return runner.ConfigureTopics(ctx, brokers, topics)
}
