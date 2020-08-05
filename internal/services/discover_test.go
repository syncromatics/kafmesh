package services_test

import (
	"context"
	"testing"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"
	"github.com/syncromatics/kafmesh/internal/services"

	"gotest.tools/assert"
)

func Test_Discover(t *testing.T) {
	service := services.DiscoverAPI{
		&discoveryv1.Service{
			Name: "test service",
		},
	}

	r, err := service.GetServiceInfo(context.Background(), &discoveryv1.GetServiceInfoRequest{})
	assert.NilError(t, err)
	assert.DeepEqual(t, r, &discoveryv1.GetServiceInfoResponse{
		Service: &discoveryv1.Service{
			Name: "test service",
		},
	})
}
