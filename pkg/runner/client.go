package runner

import (
	"context"
	"time"

	pingv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/ping/v1"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// WaitTillServiceIsRunning waits till the kafmesh service is running
func WaitTillServiceIsRunning(ctx context.Context, url string) error {
	con, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
	if err != nil {
		return errors.Wrap(err, "failed to dial service")
	}

	ping := pingv1.NewPingAPIClient(con)

	var lastErr error
	for {
		_, lastErr = ping.Ping(ctx, &pingv1.PingRequest{})
		if lastErr == nil {
			return nil
		}
		if lastErr == context.Canceled {
			return errors.Wrap(err, "timeout")
		}
		select {
		case <-ctx.Done():
			return errors.Errorf("context canceled")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
