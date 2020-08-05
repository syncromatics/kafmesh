package storage_test

import (
	"context"
	"testing"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/syncromatics/kafmesh/internal/storage"
	"gotest.tools/assert"
)

func Test_Updater(t *testing.T) {
	pod1Service := &discoveryv1.Service{
		Name:        "service1",
		Description: "this is service 1",

		Components: []*discoveryv1.Component{
			&discoveryv1.Component{
				Name:        "component1",
				Description: "this is component1",

				Sources: []*discoveryv1.Source{
					&discoveryv1.Source{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "source1.topic",
							Message: "source1.message",
						},
					},
				},
				Views: []*discoveryv1.View{
					&discoveryv1.View{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "view1.topic",
							Message: "view1.message",
						},
					},
				},
				Sinks: []*discoveryv1.Sink{
					&discoveryv1.Sink{
						Name: "sink1",
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "sink1.topic",
							Message: "sink1.message",
						},
					},
				},
				ViewSinks: []*discoveryv1.ViewSink{
					&discoveryv1.ViewSink{
						Name: "viewSink1",
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "viewSink1.topic",
							Message: "viewSink1.message",
						},
					},
				},
				ViewSources: []*discoveryv1.ViewSource{
					&discoveryv1.ViewSource{
						Name: "viewSource1",
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "viewSource1.topic",
							Message: "viewSource1.message",
						},
					},
				},
				Processors: []*discoveryv1.Processor{
					&discoveryv1.Processor{
						Name:        "processor1",
						Description: "this is processor 1",
						GroupName:   "group.1.processor",

						Persistence: &discoveryv1.Persistence{
							Topic: &discoveryv1.TopicDefinition{
								Topic:   "processor1.persistence.topic",
								Message: "processor1.persistence.message",
							},
						},
						Inputs: []*discoveryv1.Input{
							&discoveryv1.Input{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.topic",
									Message: "processor1.message",
								},
							},
							&discoveryv1.Input{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.input2.topic",
									Message: "processor1.input2.message",
								},
							},
						},
						Joins: []*discoveryv1.Join{
							&discoveryv1.Join{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.join.topic",
									Message: "processor1.join.message",
								},
							},
							&discoveryv1.Join{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.join2.topic",
									Message: "processor1.join2.message",
								},
							},
						},
						Lookups: []*discoveryv1.Lookup{
							&discoveryv1.Lookup{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.lookup.topic",
									Message: "processor1.lookup.message",
								},
							},
							&discoveryv1.Lookup{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.lookup2.topic",
									Message: "processor1.lookup2.message",
								},
							},
						},
						Outputs: []*discoveryv1.Output{
							&discoveryv1.Output{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.output.topic",
									Message: "processor1.output.message",
								},
							},
							&discoveryv1.Output{
								Topic: &discoveryv1.TopicDefinition{
									Topic:   "processor1.output2.topic",
									Message: "processor1.output2.message",
								},
							},
						},
					},
				},
			},
		},
	}

	pod2Service := &discoveryv1.Service{
		Name:        "service1",
		Description: "this is service 1",

		Components: []*discoveryv1.Component{
			&discoveryv1.Component{
				Name:        "component1",
				Description: "this is component1",

				Sources: []*discoveryv1.Source{
					&discoveryv1.Source{
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "source1.topic",
							Message: "source1.message",
						},
					},
				},
				Sinks: []*discoveryv1.Sink{
					&discoveryv1.Sink{
						Name: "sink1",
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "sink1.topic",
							Message: "sink1.message",
						},
					},
				},
				ViewSinks: []*discoveryv1.ViewSink{
					&discoveryv1.ViewSink{
						Name: "viewSink1",
						Topic: &discoveryv1.TopicDefinition{
							Topic:   "viewSink1.topic",
							Message: "viewSink1.message",
						},
					},
				},
			},
		},
	}

	updater := storage.NewUpdater(db)
	retriever := storage.NewRetriever(db)

	err := updater.Update(context.Background(), storage.Pod{Name: "pod1"}, pod1Service)
	assert.NilError(t, err)

	err = updater.Update(context.Background(), storage.Pod{Name: "pod2"}, pod2Service)
	assert.NilError(t, err)

	response, err := retriever.GetServiceForPod(context.Background(), "pod1")
	assert.NilError(t, err)
	assert.DeepEqual(t, response, pod1Service)

	response, err = retriever.GetServiceForPod(context.Background(), "pod2")
	assert.NilError(t, err)
	assert.DeepEqual(t, response, pod2Service)
}
