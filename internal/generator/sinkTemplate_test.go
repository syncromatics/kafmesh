package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func validateSink(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "enriched_data_postgres_sink.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	err = cupaloy.SnapshotMulti("validateSink", s)
	if err != nil {
		t.Fatalf("error: %s", err)
	}
}
