package services

import (
	"context"

	pingv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/ping/v1"
)

// PingAPI provides uptime status for the kafmesh service
type PingAPI struct{}

// Ping responses immediately when a request is made
func (s *PingAPI) Ping(ctx context.Context, request *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	return &pingv1.PingResponse{}, nil
}
