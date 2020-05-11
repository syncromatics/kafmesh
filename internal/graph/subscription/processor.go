package subscription

import (
	"context"
	"fmt"
	"sync"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//go:generate mockgen -source=./processor.go -destination=./processor_mock_test.go -package=subscription_test

const (
	portAnnotation = "kafmesh/port"
)

// Watcher watches a processor by key
type Watcher interface {
	Processor(ctx context.Context, in *watchv1.ProcessorRequest, opts ...grpc.CallOption) (watchv1.WatchAPI_ProcessorClient, error)
}

// ProcessorRepository is the datastore repository for processors
type ProcessorRepository interface {
	PodsByProcessors(context.Context, []int) ([][]*model.Pod, error)
	ByID(context.Context, int) (*model.Processor, error)
	ComponentByProcessors(context.Context, []int) ([]*model.Component, error)
}

var _ resolvers.ProcessorWatcher = &Processor{}

// Processor provides processor watches
type Processor struct {
	Factory             Factory
	ProcessorRepository ProcessorRepository
	PodLister           PodLister
}

// WatchProcessor watches a processor by key
func (p *Processor) WatchProcessor(ctx context.Context, input *model.WatchProcessorInput) (<-chan *model.Operation, error) {
	processor, err := p.ProcessorRepository.ByID(ctx, input.ProcessorID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor by id")
	}

	components, err := p.ProcessorRepository.ComponentByProcessors(ctx, []int{input.ProcessorID})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processor component")
	}
	if len(components) != 1 {
		return nil, errors.Errorf("did not receive correct response from components. len: %d ", len(components))
	}

	pods, err := p.ProcessorRepository.PodsByProcessors(ctx, []int{input.ProcessorID})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods from processor")
	}
	if len(pods) != 1 {
		return nil, errors.Errorf("did not receive correct response from pods. len: %d ", len(pods))
	}

	l, err := p.PodLister.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list pods")
	}

	urls := []string{}
	for _, pod := range pods[0] {
		for _, p := range l.Items {
			if p.Name != pod.Name {
				continue
			}
			port, ok := p.Annotations[portAnnotation]
			if !ok {
				port = "443"
			}

			urls = append(urls, fmt.Sprintf("%s:%s", p.Status.PodIP, port))
		}
	}
	if len(urls) == 0 {
		return nil, errors.Errorf("no pods are serving this processor")
	}

	watchRequest := &watchv1.ProcessorRequest{
		Component: components[0].Name,
		Processor: processor.Name,
		Key:       input.Key,
	}

	channels := []<-chan *watchv1.Operation{}
	group, ctx := errgroup.WithContext(ctx)
	for _, url := range urls {
		client, err := p.Factory.Client(ctx, url)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create grpc client")
		}
		c, f, err := processorWatch(ctx, client, watchRequest)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create processor watch")
		}
		group.Go(f)
		channels = append(channels, c)
	}

	return merge(channels...), nil
}

func processorWatch(ctx context.Context, client Watcher, request *watchv1.ProcessorRequest) (<-chan *watchv1.Operation, func() error, error) {
	stream, err := client.Processor(ctx, request)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to call processor on watch")
	}

	channel := make(chan *watchv1.Operation)
	return channel, func() error {
		defer close(channel)

		for {
			m, err := stream.Recv()
			if err == context.Canceled {
				return nil
			}
			if err != nil {
				return errors.Wrap(err, "failed on receive")
			}
			select {
			case <-ctx.Done():
				return nil
			case channel <- m.Operation:
			}
		}
	}, nil
}

func merge(cs ...<-chan *watchv1.Operation) <-chan *model.Operation {
	out := make(chan *model.Operation)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan *watchv1.Operation) {
			for v := range c {
				out <- mapGrpcOperationToModel(v)
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func mapGrpcOperationToModel(operation *watchv1.Operation) *model.Operation {
	result := &model.Operation{}
	result.StartTime = 0
	result.EndTime = 0
	result.Input = &model.Input{
		Topic:   operation.Input.Topic,
		Message: operation.Input.Message,
		Value:   operation.Input.Value,
	}

	for _, action := range operation.Actions {
		var m model.Action
		switch a := action.Action.(type) {
		case *watchv1.Action_ActionJoin:
			m = &model.Join{
				Message: a.ActionJoin.Message,
				Topic:   a.ActionJoin.Topic,
				Value:   a.ActionJoin.Value,
			}
		case *watchv1.Action_ActionLookup:
			m = &model.Lookup{
				Topic:   a.ActionLookup.Topic,
				Message: a.ActionLookup.Message,
				Key:     a.ActionLookup.Key,
				Value:   a.ActionLookup.Value,
			}
		case *watchv1.Action_ActionGetState:
			m = &model.GetState{
				Message: a.ActionGetState.Message,
				Topic:   a.ActionGetState.Topic,
				Value:   a.ActionGetState.Value,
			}
		case *watchv1.Action_ActionSetState:
			m = &model.SetState{
				Message: a.ActionSetState.Message,
				Topic:   a.ActionSetState.Topic,
				Value:   a.ActionSetState.Value,
			}
		case *watchv1.Action_ActionOutput:
			m = &model.Output{
				Message: a.ActionOutput.Message,
				Topic:   a.ActionOutput.Topic,
				Value:   a.ActionOutput.Value,
				Key:     a.ActionOutput.Key,
			}
		}
		result.Actions = append(result.Actions, m)
	}
	return result
}
