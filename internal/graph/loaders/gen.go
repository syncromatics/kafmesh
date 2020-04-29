package loaders

//go:generate dataloaden ServiceLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Service

//go:generate dataloaden ProcessorLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Processor
//go:generate dataloaden ProcessorSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Processor

//go:generate dataloaden SinkSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Sink

//go:generate dataloaden SourceSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Source

//go:generate dataloaden ViewSinkSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ViewSink

//go:generate dataloaden ViewSourceSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ViewSource

//go:generate dataloaden ViewSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.View

//go:generate dataloaden ComponentLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Component

//go:generate dataloaden InputSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorInput

//go:generate dataloaden JoinSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorJoin

//go:generate dataloaden LookupSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorLookup

//go:generate dataloaden OutputSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.ProcessorOutput

//go:generate dataloaden PodSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Pod

//go:generate dataloaden TopicLoader int *github.com/syncromatics/kafmesh/internal/graph/model.Topic

//go:generate dataloaden ComponentSliceLoader int []*github.com/syncromatics/kafmesh/internal/graph/model.Component
