package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func validateEmitter(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testSerial_details_source.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	err = cupaloy.SnapshotMulti("validateEmitter", s)
	if err != nil {
		t.Fatalf("error: %s", err)
	}
}
