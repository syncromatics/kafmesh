package services

import (
	"context"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
)

// DiscoverAPI provides methods for discovering and reporting kafmesh enabled pod info
type DiscoverAPI struct {
	DiscoverInfo *discoveryv1.Service
}

// GetServiceInfo is a RPC method that returns service info for discovery
func (s *DiscoverAPI) GetServiceInfo(ctx context.Context, request *discoveryv1.GetServiceInfoRequest) (*discoveryv1.GetServiceInfoResponse, error) {

	return &discoveryv1.GetServiceInfoResponse{
		Service: s.DiscoverInfo,
	}, nil
}
