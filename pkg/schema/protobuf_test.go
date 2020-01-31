package schema_test

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syncromatics/kafmesh/pkg/schema"
)

func Test_ProtobufDescribeSchema(t *testing.T) {
	p := path.Join(getPath(), "../../docs/protos")

	messages, err := schema.DescribeProtobufSchema(p)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]struct{}{
		"kafmesh.deviceId.details":   struct{}{},
		"kafmesh.deviceId.heartbeat": struct{}{},
		"kafmesh.customerId.details": struct{}{},
	}, messages)
}
