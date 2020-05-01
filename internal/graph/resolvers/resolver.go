package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
)

// DataLoaders provides data loaders for models from the context
type DataLoaders interface {
	ComponentLoader(context.Context) ComponentLoader
	PodLoader(context.Context) PodLoader
	ProcessorLoader(context.Context) ProcessorLoader
	ProcessorInputLoader(context.Context) ProcessorInputLoader
	ProcessorJoinLoader(context.Context) ProcessorJoinLoader
	ProcessorLookupLoader(context.Context) ProcessorLookupLoader
	ProcessorOutputLoader(context.Context) ProcessorOutputLoader
	QueryLoader(context.Context) QueryLoader
	ServiceLoader(context.Context) ServiceLoader
	SinkLoader(context.Context) SinkLoader
	SourceLoader(context.Context) SourceLoader
	TopicLoader(context.Context) TopicLoader
	ViewLoader(context.Context) ViewLoader
	ViewSinkLoader(context.Context) ViewSinkLoader
	ViewSourceLoader(context.Context) ViewSourceLoader
}

var _ generated.ResolverRoot = &Resolver{}

// Resolver resolvers models
type Resolver struct {
	DataLoaders DataLoaders
}

// NewResolver creates a new resolver
func NewResolver(loaders DataLoaders) *Resolver {
	return &Resolver{
		DataLoaders: loaders,
	}
}

// Query returns the query resolver
func (r *Resolver) Query() generated.QueryResolver {
	return &QueryResolver{r}
}

// Service returns the Service resolver
func (r *Resolver) Service() generated.ServiceResolver {
	return &ServiceResolver{r}
}

// Component returns the component resolver
func (r *Resolver) Component() generated.ComponentResolver {
	return &ComponentResolver{r}
}

// Pod returns the pod resolver
func (r *Resolver) Pod() generated.PodResolver {
	return &PodResolver{r}
}

// Processor returns the processor resolver
func (r *Resolver) Processor() generated.ProcessorResolver {
	return &ProcessorResolver{r}
}

// ProcessorInput returns the processor input resolver
func (r *Resolver) ProcessorInput() generated.ProcessorInputResolver {
	return &ProcessorInputResolver{r}
}

// ProcessorJoin returns the processor join resolver
func (r *Resolver) ProcessorJoin() generated.ProcessorJoinResolver {
	return &ProcessorJoinResolver{r}
}

// ProcessorLookup returns the processor lookupu resolver
func (r *Resolver) ProcessorLookup() generated.ProcessorLookupResolver {
	return &ProcessorLookupResolver{r}
}

// ProcessorOutput returns the processor output resolver
func (r *Resolver) ProcessorOutput() generated.ProcessorOutputResolver {
	return &ProcessorOutputResolver{r}
}

// Sink returns the sink resolver
func (r *Resolver) Sink() generated.SinkResolver {
	return &SinkResolver{r}
}

// Source returns the source resolver
func (r *Resolver) Source() generated.SourceResolver {
	return &SourceResolver{r}
}

// Topic returns the topic resolver
func (r *Resolver) Topic() generated.TopicResolver {
	return &TopicResolver{r}
}

// View returns the view resolver
func (r *Resolver) View() generated.ViewResolver {
	return &ViewResolver{r}
}

// ViewSink returns the view sink resolver
func (r *Resolver) ViewSink() generated.ViewSinkResolver {
	return &ViewSinkResolver{r}
}

// ViewSource returns the view source resolver
func (r *Resolver) ViewSource() generated.ViewSourceResolver {
	return &ViewSourceResolver{r}
}
