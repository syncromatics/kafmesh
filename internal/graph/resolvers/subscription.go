package resolvers

import (
	"context"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

//go:generate mockgen -source=./subscription.go -destination=./subscription_mock_test.go -package=resolvers_test

// ProcessorWatcher handles processor subscriptions
type ProcessorWatcher interface {
	WatchProcessor(context.Context, *model.WatchProcessorInput) (<-chan *model.Operation, error)
}

var _ generated.SubscriptionResolver = &Subscription{}

// Subscription is the subscription resolver
type Subscription struct {
	*Resolver
}

// WatchProcessor observes a processor by key
func (s *Subscription) WatchProcessor(ctx context.Context, input *model.WatchProcessorInput) (<-chan *model.Operation, error) {
	return s.Subscribers.Processor().WatchProcessor(ctx, input)
}
