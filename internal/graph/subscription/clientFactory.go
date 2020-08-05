package subscription

import (
	"context"

	watchv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/watch/v1"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// ClientFactory generates WatchClients for a pod
type ClientFactory struct{}

// Client gets a Watch client for an address
func (f *ClientFactory) Client(ctx context.Context, url string) (Watcher, error) {
	con, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial service")
	}

	go func() {
		<-ctx.Done()
		con.Close()
	}()

	return watchv1.NewWatchAPIClient(con), nil
}
