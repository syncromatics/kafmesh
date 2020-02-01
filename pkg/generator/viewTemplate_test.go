package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateView(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "kafmesh", "details", "testMesh_testSerial_detailsEnriched_view.km.go"))
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

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/syncromatics/kafmesh/pkg/runner"
	"test/kafmesh/models/testMesh/testSerial"
)

type TestSerial_DetailsEnriched_View struct {
	view *goka.View
}

func New_TestSerial_DetailsEnriched_View(options runner.ServiceOptions) (*TestSerial_DetailsEnriched_View, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("testMesh.testSerial.detailsEnriched", &testSerial.DetailsEnriched{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	var builder storage.Builder
	if g.settings.StorageInMemory {
		builder = storage.MemoryBuilder()
	} else {
		opts := &opt.Options{
			BlockCacheCapacity: opt.MiB * 1,
			WriteBuffer:        opt.MiB * 1,
		}

		path := filepath.Join("/tmp/storage", "view", "testMesh.testSerial.detailsEnriched")

		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create view db directory")
		}

		builder = storage.BuilderWithOptions(path, opts)
	}

	view, err := goka.NewView(g.settings.Brokers,
		goka.Table("testMesh.testSerial.detailsEnriched"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating view")
	}

	return &TestSerial_DetailsEnriched_View{
		view: view,
	}, nil
}

func (v *TestSerial_DetailsEnriched_View) Watch(ctx context.Context) func() error {
	return v.view.Run(ctx)
}

func (v *TestSerial_DetailsEnriched_View) Keys() []string {
	return v.Keys()
}

func (v *TestSerial_DetailsEnriched_View) Get(key string) (*testSerial.DetailsEnriched, error) {
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
