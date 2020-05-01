package loaders

import (
	"context"
	"net/http"

	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

// Repositories is a collection of all data repositories
type Repositories interface {
	Component() ComponentRepository
	Service() ServiceRepository
	Processor() ProcessorRepository
	ProcessorInput() ProcessorInputRepository
	ProcessorJoin() ProcessorJoinRepository
	ProcessorLookup() ProcessorLookupRepository
	ProcessorOutput() ProcessorOutputRepository
	Sink() SinkRepository
	Source() SourceRepository
	ViewSink() ViewSinkRepository
	ViewSource() ViewSourceRepository
	View() ViewRepository
	Pod() PodRepository
	Topic() TopicRepository
	Query() QueryRepository
}

type ctxKeyType struct{ name string }

var ctxKey = ctxKeyType{"loadersCtx"}

// Loaders is a collection of model loaders
type Loaders struct {
	ComponentLoader       *ComponentLoader
	ServiceLoader         *ServiceLoader
	ProcessorLoader       *ProcessorLoader
	ProcessorInputLoader  *ProcessorInputLoader
	ProcessorJoinLoader   *ProcessorJoinLoader
	ProcessorLookupLoader *ProcessorLookupLoader
	ProcessorOutputLoader *ProcessorOutputLoader
	SinkLoader            *SinkLoader
	SourceLoader          *SourceLoader
	ViewSinkLoader        *ViewSinkLoader
	ViewSourceLoader      *ViewSourceLoader
	ViewLoader            *ViewLoader
	PodLoader             *PodLoader
	TopicLoader           *TopicLoader
	QueryLoader           *QueryLoader
}

// NewLoaders creates a new Loaders
func NewLoaders(ctx context.Context, repositories Repositories) *Loaders {
	return &Loaders{
		ComponentLoader:       NewComponentLoader(ctx, repositories.Component()),
		ServiceLoader:         NewServiceLoader(ctx, repositories.Service()),
		ProcessorLoader:       NewProcessorLoader(ctx, repositories.Processor()),
		ProcessorInputLoader:  NewProcessorInputLoader(ctx, repositories.ProcessorInput()),
		ProcessorJoinLoader:   NewProcessorJoinLoader(ctx, repositories.ProcessorJoin()),
		ProcessorLookupLoader: NewProcessorLookupLoader(ctx, repositories.ProcessorLookup()),
		ProcessorOutputLoader: NewProcessorOutputLoader(ctx, repositories.ProcessorOutput()),
		SinkLoader:            NewSinkLoader(ctx, repositories.Sink()),
		SourceLoader:          NewSourceLoader(ctx, repositories.Source()),
		ViewSinkLoader:        NewViewSinkLoader(ctx, repositories.ViewSink()),
		ViewSourceLoader:      NewViewSourceLoader(ctx, repositories.ViewSource()),
		PodLoader:             NewPodLoader(ctx, repositories.Pod()),
		TopicLoader:           NewTopicLoader(ctx, repositories.Topic()),
		QueryLoader:           NewQueryLoader(ctx, repositories.Query()),
		ViewLoader:            NewViewLoader(ctx, repositories.View()),
	}
}

// NewMiddleware wires up the dataloaders into the http pipeline
func NewMiddleware(repositories Repositories) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			loaders := NewLoaders(r.Context(), repositories)
			dlCtx := context.WithValue(r.Context(), ctxKey, loaders)
			next.ServeHTTP(w, r.WithContext(dlCtx))
		})
	}
}

var _ resolvers.DataLoaders = &LoaderFactory{}

// LoaderFactory gets the data loaders from the context
type LoaderFactory struct{}

func (f *LoaderFactory) ctxLoaders(ctx context.Context) *Loaders {
	return ctx.Value(ctxKey).(*Loaders)
}

// ComponentLoader returns the component data loader
func (f *LoaderFactory) ComponentLoader(ctx context.Context) resolvers.ComponentLoader {
	return f.ctxLoaders(ctx).ComponentLoader
}

// PodLoader returns the pod data loader
func (f *LoaderFactory) PodLoader(ctx context.Context) resolvers.PodLoader {
	return f.ctxLoaders(ctx).PodLoader
}

// ProcessorInputLoader returns the processor input data loader
func (f *LoaderFactory) ProcessorInputLoader(ctx context.Context) resolvers.ProcessorInputLoader {
	return f.ctxLoaders(ctx).ProcessorInputLoader
}

// ProcessorJoinLoader returns the processor join data loader
func (f *LoaderFactory) ProcessorJoinLoader(ctx context.Context) resolvers.ProcessorJoinLoader {
	return f.ctxLoaders(ctx).ProcessorJoinLoader
}

// ProcessorLookupLoader returns the processor lookup data loader
func (f *LoaderFactory) ProcessorLookupLoader(ctx context.Context) resolvers.ProcessorLookupLoader {
	return f.ctxLoaders(ctx).ProcessorLookupLoader
}

// ProcessorOutputLoader returns the processor output data loader
func (f *LoaderFactory) ProcessorOutputLoader(ctx context.Context) resolvers.ProcessorOutputLoader {
	return f.ctxLoaders(ctx).ProcessorOutputLoader
}

// ProcessorLoader returns the processor data loader
func (f *LoaderFactory) ProcessorLoader(ctx context.Context) resolvers.ProcessorLoader {
	return f.ctxLoaders(ctx).ProcessorLoader
}

// QueryLoader returns the query data loader
func (f *LoaderFactory) QueryLoader(ctx context.Context) resolvers.QueryLoader {
	return f.ctxLoaders(ctx).QueryLoader
}

// ServiceLoader returns the service data loader
func (f *LoaderFactory) ServiceLoader(ctx context.Context) resolvers.ServiceLoader {
	return f.ctxLoaders(ctx).ServiceLoader
}

// SinkLoader returns the sink data loader
func (f *LoaderFactory) SinkLoader(ctx context.Context) resolvers.SinkLoader {
	return f.ctxLoaders(ctx).SinkLoader
}

// SourceLoader returns the source data loader
func (f *LoaderFactory) SourceLoader(ctx context.Context) resolvers.SourceLoader {
	return f.ctxLoaders(ctx).SourceLoader
}

// TopicLoader returns the topic data loader
func (f *LoaderFactory) TopicLoader(ctx context.Context) resolvers.TopicLoader {
	return f.ctxLoaders(ctx).TopicLoader
}

// ViewLoader returns the view data loader
func (f *LoaderFactory) ViewLoader(ctx context.Context) resolvers.ViewLoader {
	return f.ctxLoaders(ctx).ViewLoader
}

// ViewSinkLoader returns the view sink data loader
func (f *LoaderFactory) ViewSinkLoader(ctx context.Context) resolvers.ViewSinkLoader {
	return f.ctxLoaders(ctx).ViewSinkLoader
}

// ViewSourceLoader returns the view source data loader
func (f *LoaderFactory) ViewSourceLoader(ctx context.Context) resolvers.ViewSourceLoader {
	return f.ctxLoaders(ctx).ViewSourceLoader
}
