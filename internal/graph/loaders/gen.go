package loaders

//go:generate dataloaden serviceLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Service

//go:generate dataloaden processorLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Processor
//go:generate dataloaden processorSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Processor

//go:generate dataloaden sinkSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Sink

//go:generate dataloaden sourceSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Source

//go:generate dataloaden viewSinkSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ViewSink

//go:generate dataloaden viewSourceSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ViewSource

//go:generate dataloaden viewSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.View

//go:generate dataloaden componentLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Component

//go:generate dataloaden inputSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorInput

//go:generate dataloaden joinSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorJoin

//go:generate dataloaden lookupSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorLookup

//go:generate dataloaden outputSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorOutput

//go:generate dataloaden podSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Pod

//go:generate dataloaden topicLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Topic

//go:generate dataloaden componentSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Component
