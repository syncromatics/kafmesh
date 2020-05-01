package repositories

import (
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
)

var _ loaders.Repositories = &AllRepositories{}

// AllRepositories contains all repositories
type AllRepositories struct {
	component       *Component
	pod             *Pod
	processor       *Processor
	processorInput  *ProcessorInput
	processorJoin   *ProcessorJoin
	processorLookup *ProcessorLookup
	processorOutput *ProcessorOutput
	query           *Query
	service         *Service
	sink            *Sink
	source          *Source
	topic           *Topic
	view            *View
	viewSink        *ViewSink
	viewSource      *ViewSource
}

// All creates a struct with all repositories
func All(db *sql.DB) *AllRepositories {
	return &AllRepositories{
		component:       &Component{db},
		pod:             &Pod{db},
		processor:       &Processor{db},
		processorInput:  &ProcessorInput{db},
		processorJoin:   &ProcessorJoin{db},
		processorLookup: &ProcessorLookup{db},
		processorOutput: &ProcessorOutput{db},
		query:           &Query{db},
		service:         &Service{db},
		sink:            &Sink{db},
		source:          &Source{db},
		topic:           &Topic{db},
		view:            &View{db},
		viewSink:        &ViewSink{db},
		viewSource:      &ViewSource{db},
	}
}

// Component returns the component repository
func (a *AllRepositories) Component() loaders.ComponentRepository {
	return a.component
}

// Pod returns the pod repository
func (a *AllRepositories) Pod() loaders.PodRepository {
	return a.pod
}

// Processor returns the processor repository
func (a *AllRepositories) Processor() loaders.ProcessorRepository {
	return a.processor
}

// ProcessorInput returns the processor input repository
func (a *AllRepositories) ProcessorInput() loaders.ProcessorInputRepository {
	return a.processorInput
}

// ProcessorJoin returns the processor join repository
func (a *AllRepositories) ProcessorJoin() loaders.ProcessorJoinRepository {
	return a.processorJoin
}

// ProcessorLookup returns the processor lookup repository
func (a *AllRepositories) ProcessorLookup() loaders.ProcessorLookupRepository {
	return a.processorLookup
}

// ProcessorOutput returns the processor output repository
func (a *AllRepositories) ProcessorOutput() loaders.ProcessorOutputRepository {
	return a.processorOutput
}

// Query returns the query repository
func (a *AllRepositories) Query() loaders.QueryRepository {
	return a.query
}

// Service returns the service repository
func (a *AllRepositories) Service() loaders.ServiceRepository {
	return a.service
}

// Sink returns the sink repository
func (a *AllRepositories) Sink() loaders.SinkRepository {
	return a.sink
}

// Source returns the source repository
func (a *AllRepositories) Source() loaders.SourceRepository {
	return a.source
}

// Topic returns the topic repository
func (a *AllRepositories) Topic() loaders.TopicRepository {
	return a.topic
}

// View returns the view repository
func (a *AllRepositories) View() loaders.ViewRepository {
	return a.view
}

// ViewSink returns the view sink repository
func (a *AllRepositories) ViewSink() loaders.ViewSinkRepository {
	return a.viewSink
}

// ViewSource returns the view source repository
func (a *AllRepositories) ViewSource() loaders.ViewSourceRepository {
	return a.viewSource
}
