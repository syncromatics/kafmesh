package resolvers_test

import (
	"context"
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/model"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
	"gotest.tools/assert"

	gomock "github.com/golang/mock/gomock"
)

func Test_Subscription_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	subscriptions := NewMockSubscribers(ctrl)

	processorSubscriber := NewMockProcessorWatcher(ctrl)

	subscriptions.EXPECT().
		Processor().
		Return(processorSubscriber).
		Times(1)

	processorSubscriber.EXPECT().
		WatchProcessor(gomock.Any(), &model.WatchProcessorInput{}).
		Return(nil, nil).
		Times(1)

	resolver := &resolvers.Resolver{
		Subscribers: subscriptions,
	}

	_, err := resolver.Subscription().WatchProcessor(context.Background(), &model.WatchProcessorInput{})
	assert.NilError(t, err)
}
