package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/loaders"
)

type loaderFunc func(context.Context) *loaders.Loaders

var _ generated.ResolverRoot = &Resolver{}

// Resolver resolvers models
type Resolver struct {
	query   *QueryResolver
	loaders loaderFunc
}

// NewResolver creates a new resolver
func NewResolver(loaders loaderFunc) *Resolver {
	return &Resolver{
		loaders: loaders,
		query: &QueryResolver{
			loaderFunc: loaders,
		},
	}
}

// Query returns the query resolver
func (r *Resolver) Query() generated.QueryResolver {
	return r.query
}

// Service returns the Service resolver
func (r *Resolver) Service() generated.ServiceResolver {
	return &ServiceResolver{r.loaders}
}

// Component returns the component resolver
func (r *Resolver) Component() generated.ComponentResolver {
	return &ComponentResolver{r.loaders}
}

// Pod returns the pod resolver
func (r *Resolver) Pod() generated.PodResolver {
	return &PodResolver{r.loaders}
}

// Processor returns the processor resolver
func (r *Resolver) Processor() generated.ProcessorResolver {
	return &ProcessorResolver{r.loaders}
}

// ProcessorInput returns the processor input resolver
func (r *Resolver) ProcessorInput() generated.ProcessorInputResolver {
	return &ProcessorInputResolver{r.loaders}
}

// ProcessorJoin returns the processor join resolver
func (r *Resolver) ProcessorJoin() generated.ProcessorJoinResolver {
	return &ProcessorJoinResolver{r.loaders}
}

// ProcessorLookup returns the processor lookupu resolver
func (r *Resolver) ProcessorLookup() generated.ProcessorLookupResolver {
	return &ProcessorLookupResolver{r.loaders}
}

// ProcessorOutput returns the processor output resolver
func (r *Resolver) ProcessorOutput() generated.ProcessorOutputResolver {
	return &ProcessorOutputResolver{r.loaders}
}

// Sink returns the sink resolver
func (r *Resolver) Sink() generated.SinkResolver {
	return &SinkResolver{r.loaders}
}

// Source returns the source resolver
func (r *Resolver) Source() generated.SourceResolver {
	return &SourceResolver{r.loaders}
}

// Topic returns the topic resolver
func (r *Resolver) Topic() generated.TopicResolver {
	return &TopicResolver{r.loaders}
}

// View returns the view resolver
func (r *Resolver) View() generated.ViewResolver {
	return &ViewResolver{r.loaders}
}

// ViewSink returns the view sink resolver
func (r *Resolver) ViewSink() generated.ViewSinkResolver {
	return &ViewSinkResolver{r.loaders}
}

// ViewSource returns the view source resolver
func (r *Resolver) ViewSource() generated.ViewSourceResolver {
	return &ViewSourceResolver{r.loaders}
}
