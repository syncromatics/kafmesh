package services_test

import (
	"context"
	"testing"

	pingv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/ping/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	"gotest.tools/assert"
)

func Test_Ping(t *testing.T) {
	service := services.PingAPI{}

	r, err := service.Ping(context.Background(), &pingv1.PingRequest{})
	assert.NilError(t, err)

	assert.DeepEqual(t, r, &pingv1.PingResponse{})
}
