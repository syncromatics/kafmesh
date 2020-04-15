package scraper

import (
	"context"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// ClientFactory generates DiscoveryClients for a pod
type ClientFactory struct{}

// Client gets a Discovery client for an address
func (f *ClientFactory) Client(ctx context.Context, url string) (DiscoveryClient, func() error, error) {
	con, err := grpc.DialContext(ctx, url, grpc.WithInsecure())
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to dial service")
	}

	return discoveryv1.NewDiscoveryAPIClient(con), con.Close, nil
}
