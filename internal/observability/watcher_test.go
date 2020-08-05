package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/syncromatics/kafmesh/internal/observability"
	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"gotest.tools/assert"
)

func Test_Watcher(t *testing.T) {
	watcher := &observability.Watcher{}

	ctx, cancel := context.WithCancel(context.Background())

	operation := &watchv1.Operation{}

	watch1 := false
	watch2 := false
	go func() {
		watcher.WatchProcessor(ctx, &watchv1.ProcessorRequest{
			Component: "com1",
			Processor: "proc1",
			Key:       "12",
		}, func(m *watchv1.ProcessorResponse) error {
			assert.DeepEqual(t, m, &watchv1.ProcessorResponse{
				Operation: operation,
			})
			watch1 = true
			return nil
		})
	}()
	go func() {
		watcher.WatchProcessor(ctx, &watchv1.ProcessorRequest{
			Component: "com1",
			Processor: "proc1",
			Key:       "12",
		}, func(m *watchv1.ProcessorResponse) error {
			assert.DeepEqual(t, m, &watchv1.ProcessorResponse{
				Operation: operation,
			})
			watch2 = true
			return nil
		})
	}()

	for {
		i, ok := watcher.WatchCount("com1", "proc1", "12")
		if ok && i == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	watcher.Send("com1", "proc1", "12", operation)

	cancel()

	assert.Assert(t, watch1)
	assert.Assert(t, watch2)
}
