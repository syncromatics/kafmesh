package schema_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/syncromatics/kafmesh/internal/schema"

	"github.com/stretchr/testify/assert"
)

func Test_ProtobufDescribeSchema(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "Test_ProtobufDescribeSchema")
	if err != nil {
		t.Fatal(err)
	}
	tmpDir = path.Join(tmpDir, "protos")

	err = os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	package1 := path.Join(tmpDir, "package1")
	err = os.MkdirAll(package1, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(path.Join(package1, "test.proto"), []byte(`syntax ="proto3";
package package1.sub1;

message Test {
	string name = 1;
}`), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(path.Join(tmpDir, "package1", "test2.proto"), []byte(`syntax ="proto3";
package package1.sub1;

import "google/protobuf/timestamp.proto";

message Test2 {
	string serial = 1;
	google.protobuf.Timestamp time = 2;
}`), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	messages, err := schema.DescribeProtobufSchema(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]struct{}{
		"package1.sub1.test":  struct{}{},
		"package1.sub1.test2": struct{}{},
	}, messages)
}
