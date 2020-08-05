package subscription_test

import (
	"testing"

	"github.com/syncromatics/kafmesh/internal/graph/subscription"

	"github.com/golang/mock/gomock"
	"gotest.tools/assert"
)

func Test_Subscribers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lister := NewMockPodLister(ctrl)
	repo := NewMockProcessorRepository(ctrl)

	subscriber := subscription.NewSubscribers(lister, repo)

	proc := subscriber.Processor()
	assert.Assert(t, proc != nil)
}
