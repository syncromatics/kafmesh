package subscription_test

import (
	"context"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	subscription "github.com/syncromatics/kafmesh/internal/graph/subscription"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"
	"google.golang.org/grpc/metadata"

	"github.com/golang/mock/gomock"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_Processor_WatchProcessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewMockFactory(ctrl)
	repository := NewMockProcessorRepository(ctrl)
	lister := NewMockPodLister(ctrl)

	repository.EXPECT().
		ByID(gomock.Any(), 12).
		Return(&model.Processor{
			Name: "testProcessor",
		}, nil).
		Times(1)

	repository.EXPECT().
		ComponentByProcessors(gomock.Any(), []int{12}).
		Return([]*model.Component{
			&model.Component{
				Name: "component1",
			},
		}, nil).
		Times(1)

	repository.EXPECT().
		PodsByProcessors(gomock.Any(), []int{12}).
		Return([][]*model.Pod{
			[]*model.Pod{
				&model.Pod{Name: "pod1"},
				&model.Pod{Name: "pod2"},
			},
		}, nil).
		Times(1)

	lister.EXPECT().
		List(gomock.Any(), gomock.Any()).
		Return(&corev1.PodList{
			Items: []corev1.Pod{
				corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod1",
						Annotations: map[string]string{
							"kafmesh/port": "7777",
						},
					},
					Status: v1.PodStatus{
						PodIP: "1.1.1.1",
					},
				},
				corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "pod2",
						Annotations: map[string]string{},
					},
					Status: v1.PodStatus{
						PodIP: "1.1.1.2",
					},
				},
			},
		}, nil).
		Times(1)

	client1 := NewMockWatcher(ctrl)
	client1.EXPECT().
		Processor(gomock.Any(), &watchv1.ProcessorRequest{
			Component: "component1",
			Processor: "testProcessor",
			Key:       "tester",
		}).
		DoAndReturn(func(ctx context.Context, request *watchv1.ProcessorRequest) (watchv1.WatchAPI_ProcessorClient, error) {
			return &sub{
				ctx: ctx,
				messages: []*watchv1.ProcessorResponse{
					&watchv1.ProcessorResponse{
						Operation: &watchv1.Operation{
							Input: &watchv1.Input{
								Topic:   "topic1",
								Message: "message1",
								Value:   "input1",
							},
							Actions: []*watchv1.Action{
								&watchv1.Action{
									Action: &watchv1.Action_ActionJoin{
										ActionJoin: &watchv1.Join{
											Topic:   "join1_topic",
											Message: "join1_message",
											Value:   "join1_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionLookup{
										ActionLookup: &watchv1.Lookup{
											Topic:   "lookup1_topic",
											Message: "lookup1_message",
											Key:     "lookup1_key",
											Value:   "lookup1_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionGetState{
										ActionGetState: &watchv1.GetState{
											Topic:   "getState1_topic",
											Message: "getState1_message",
											Value:   "getState1_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionSetState{
										ActionSetState: &watchv1.SetState{
											Topic:   "setState1_topic",
											Message: "setState1_message",
											Value:   "setState1_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionOutput{
										ActionOutput: &watchv1.Output{
											Topic:   "output1_topic",
											Message: "output1_message",
											Key:     "output1_key",
											Value:   "output1_value",
										},
									},
								},
							},
						},
					},
				},
			}, nil
		}).
		Times(1)

	factory.EXPECT().
		Client(gomock.Any(), "1.1.1.1:7777").
		Return(client1, nil).
		Times(1)

	client2 := NewMockWatcher(ctrl)
	client2.EXPECT().
		Processor(gomock.Any(), &watchv1.ProcessorRequest{
			Component: "component1",
			Processor: "testProcessor",
			Key:       "tester",
		}).
		DoAndReturn(func(ctx context.Context, request *watchv1.ProcessorRequest) (watchv1.WatchAPI_ProcessorClient, error) {
			return &sub{
				ctx: ctx,
				messages: []*watchv1.ProcessorResponse{
					&watchv1.ProcessorResponse{
						Operation: &watchv1.Operation{
							Input: &watchv1.Input{
								Topic:   "topic2",
								Message: "message2",
								Value:   "input2",
							},
							Actions: []*watchv1.Action{
								&watchv1.Action{
									Action: &watchv1.Action_ActionJoin{
										ActionJoin: &watchv1.Join{
											Topic:   "join2_topic",
											Message: "join2_message",
											Value:   "join2_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionLookup{
										ActionLookup: &watchv1.Lookup{
											Topic:   "lookup2_topic",
											Message: "lookup2_message",
											Key:     "lookup2_key",
											Value:   "lookup2_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionGetState{
										ActionGetState: &watchv1.GetState{
											Topic:   "getState2_topic",
											Message: "getState2_message",
											Value:   "getState2_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionSetState{
										ActionSetState: &watchv1.SetState{
											Topic:   "setState2_topic",
											Message: "setState2_message",
											Value:   "setState2_value",
										},
									},
								},
								&watchv1.Action{
									Action: &watchv1.Action_ActionOutput{
										ActionOutput: &watchv1.Output{
											Topic:   "output2_topic",
											Message: "output2_message",
											Key:     "output2_key",
											Value:   "output2_value",
										},
									},
								},
							},
						},
					},
				},
			}, nil
		}).
		Times(1)

	factory.EXPECT().
		Client(gomock.Any(), "1.1.1.2:443").
		Return(client2, nil).
		Times(1)

	watcher := &subscription.Processor{
		Factory:             factory,
		ProcessorRepository: repository,
		PodLister:           lister,
	}

	ctx, cancel := context.WithCancel(context.Background())

	r, err := watcher.WatchProcessor(ctx, &model.WatchProcessorInput{
		ProcessorID: 12,
		Key:         "tester",
	})
	assert.NilError(t, err)

	checkMessage := func(msg *model.Operation) {
		var expected *model.Operation
		switch msg.Input.Topic {
		case "topic1":
			expected = &model.Operation{
				StartTime: 0,
				EndTime:   0,
				Input: &model.Input{
					Topic:   "topic1",
					Message: "message1",
					Value:   "input1",
				},
				Actions: []model.Action{
					&model.Join{Topic: "join1_topic", Message: "join1_message", Value: "join1_value"},
					&model.Lookup{Topic: "lookup1_topic", Message: "lookup1_message", Value: "lookup1_value", Key: "lookup1_key"},
					&model.GetState{Topic: "getState1_topic", Message: "getState1_message", Value: "getState1_value"},
					&model.SetState{Topic: "setState1_topic", Message: "setState1_message", Value: "setState1_value"},
					&model.Output{Topic: "output1_topic", Message: "output1_message", Value: "output1_value", Key: "output1_key"},
				},
			}
		case "topic2":
			expected = &model.Operation{
				StartTime: 0,
				EndTime:   0,
				Input: &model.Input{
					Topic:   "topic2",
					Message: "message2",
					Value:   "input2",
				},
				Actions: []model.Action{
					&model.Join{Topic: "join2_topic", Message: "join2_message", Value: "join2_value"},
					&model.Lookup{Topic: "lookup2_topic", Message: "lookup2_message", Value: "lookup2_value", Key: "lookup2_key"},
					&model.GetState{Topic: "getState2_topic", Message: "getState2_message", Value: "getState2_value"},
					&model.SetState{Topic: "setState2_topic", Message: "setState2_message", Value: "setState2_value"},
					&model.Output{Topic: "output2_topic", Message: "output2_message", Value: "output2_value", Key: "output2_key"},
				},
			}
		}
		assert.DeepEqual(t, msg, expected)
	}

	timer := time.NewTimer(1 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("failed waiting for message")
	case m := <-r:
		checkMessage(m)
	}

	timer = time.NewTimer(1 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("failed waiting for message")
	case m := <-r:
		checkMessage(m)
	}

	cancel()
}

var _ watchv1.WatchAPI_ProcessorClient = &sub{}

type sub struct {
	ctx      context.Context
	messages []*watchv1.ProcessorResponse
	index    int
}

func (*sub) CloseSend() error {
	return nil
}

func (s *sub) Context() context.Context {
	return s.ctx
}

func (*sub) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *sub) Recv() (*watchv1.ProcessorResponse, error) {
	if s.index > len(s.messages)-1 {
		<-s.ctx.Done()
		return nil, context.Canceled
	}
	m := s.messages[s.index]
	s.index++
	return m, nil
}

func (*sub) RecvMsg(interface{}) error {
	return nil
}

func (*sub) SendMsg(interface{}) error {
	return nil
}

func (*sub) Trailer() metadata.MD {
	return nil
}
