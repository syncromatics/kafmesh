package runner

import "github.com/prometheus/client_golang/prometheus"

// Metrics handles prometheus metrics
type Metrics struct {
	sourceCount  *prometheus.CounterVec
	sourceErrors *prometheus.CounterVec
}

// NewMetrics creates a new metrics handler
func NewMetrics() *Metrics {
	sourceCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafmesh_source_count",
			Help: "How many messages are published to a source topic.",
		},
		[]string{"service", "component", "topic"},
	)
	prometheus.MustRegister(sourceCount)

	sourceErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafmesh_source_errors",
			Help: "Failures to publish to a source topic.",
		},
		[]string{"service", "component", "topic"},
	)
	prometheus.MustRegister(sourceErrors)

	return &Metrics{
		sourceCount:  sourceCount,
		sourceErrors: sourceErrors,
	}
}

// SourceHit records how many messages are published to a source topic
func (m *Metrics) SourceHit(service, component, topic string, count int) {
	m.sourceCount.WithLabelValues(service, component, topic).Add(float64(count))
}

// SourceError records an error publishing to a source topic
func (m *Metrics) SourceError(service, component, topic string) {
	m.sourceErrors.WithLabelValues(service, component, topic).Inc()
}
