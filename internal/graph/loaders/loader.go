package loaders

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/pkg/errors"
)

type ctxKeyType struct{ name string }

var ctxKey = ctxKeyType{"loadersCtx"}

// Loaders is a collection of model loaders
type Loaders struct {
	ComponentLoader       *Components
	ServiceLoader         *Services
	ProcessorLoader       *Processors
	ProcessorInputLoader  *ProcessorInputs
	ProcessorJoinLoader   *ProcessorJoins
	ProcessorLookupLoader *ProcessorLookups
	ProcessorOutputLoader *ProcessorOutputs
	SinkLoader            *Sinks
	SourceLoader          *Sources
	ViewSinkLoader        *ViewSinks
	ViewSourceLoader      *ViewSources
	ViewLoader            *Views
	PodLoader             *Pods
	TopicLoader           *Topics

	db      *sql.DB
	context context.Context
}

// NewLoaders creates a new Loaders
func NewLoaders(ctx context.Context, db *sql.DB) *Loaders {
	loaders := &Loaders{
		db:      db,
		context: ctx,
	}
	configureServices(loaders)
	configureComponentLoaders(loaders)
	configureProcessors(loaders)
	configureProcessorInputs(loaders)
	configureProcessorJoins(loaders)
	configureProcessorLookups(loaders)
	configureProcessorOutputs(loaders)
	configureSinks(loaders)
	configureSources(loaders)
	configureViewSinks(loaders)
	configureViewSources(loaders)
	configureViews(loaders)
	configurePods(loaders)
	configureTopics(loaders)

	return loaders
}

// NewMiddleware wires up the dataloaders into the http pipeline
func NewMiddleware(db *sql.DB) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loaders := NewLoaders(r.Context(), db)
			dlCtx := context.WithValue(r.Context(), ctxKey, loaders)
			next.ServeHTTP(w, r.WithContext(dlCtx))
		})
	}
}

// CtxLoaders gets the data loaders object from the context
func CtxLoaders(ctx context.Context) *Loaders {
	return ctx.Value(ctxKey).(*Loaders)
}

// GetAllServices gets all services
func (s *Loaders) GetAllServices() ([]*model.Service, error) {
	rows, err := s.db.QueryContext(s.context, `select id, name, description from services`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query services")
	}
	defer rows.Close()

	services := []*model.Service{}
	for rows.Next() {
		service := &model.Service{}
		err = rows.Scan(&service.ID, &service.Name, &service.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan service")
		}
		services = append(services, service)
	}

	return services, nil
}

// GetAllPods gets all pods
func (s *Loaders) GetAllPods() ([]*model.Pod, error) {
	rows, err := s.db.QueryContext(s.context, `select id, name from pods`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query pods")
	}
	defer rows.Close()

	pods := []*model.Pod{}
	for rows.Next() {
		pod := &model.Pod{}
		err = rows.Scan(&pod.ID, &pod.Name)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan pod")
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

// GetAllTopics gets all topics
func (s *Loaders) GetAllTopics() ([]*model.Topic, error) {
	rows, err := s.db.QueryContext(s.context, `select id, name, message from topics`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query topics")
	}
	defer rows.Close()

	results := []*model.Topic{}
	for rows.Next() {
		topic := &model.Topic{}
		err = rows.Scan(&topic.ID, &topic.Name, &topic.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan topic")
		}
		results = append(results, topic)
	}

	return results, nil
}
