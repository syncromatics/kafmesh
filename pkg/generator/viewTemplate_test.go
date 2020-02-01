package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateView(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testMesh_testSerial_detailsEnriched_view.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedView, string(s))
}

var (
	expectedView = `// Code generated by kafmesh-gen. DO NOT EDIT.

package details

import (
	"context"
	"os"
	"path/filepath"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/syncromatics/kafmesh/pkg/runner"

	testSerial "test/internal/kafmesh/models/testMesh/testSerial"
)

type TestSerialDetailsEnriched_View struct {
	view *goka.View
}

func New_TestSerialDetailsEnriched_View(options runner.ServiceOptions) (*TestSerialDetailsEnriched_View, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("testMesh.testSerial.detailsEnriched", &testSerial.DetailsEnriched{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "view", "testMesh.testSerial.detailsEnriched")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create view db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)

	view, err := goka.NewView(brokers,
		goka.Table("testMesh.testSerial.detailsEnriched"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating view")
	}

	return &TestSerialDetailsEnriched_View{
		view: view,
	}, nil
}

func (v *TestSerialDetailsEnriched_View) Watch(ctx context.Context) func() error {
	return func() error {
		return v.view.Run(ctx)
	}
}

func (v *TestSerialDetailsEnriched_View) Keys() []string {
	return v.Keys()
}

func (v *TestSerialDetailsEnriched_View) Get(key string) (*testSerial.DetailsEnriched, error) {
	m, err := v.view.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get value from view")
	}

	msg, ok := m.(*testSerial.DetailsEnriched)
	if !ok {
		return nil, errors.Errorf("expecting message of type '*testSerial.DetailsEnriched' got type '%t'", m)
	}

	return msg, nil
}
`
)
