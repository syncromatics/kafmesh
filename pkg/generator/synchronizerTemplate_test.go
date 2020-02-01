package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateSynchronizer(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testMesh_testId_test_synchronizer.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedSynchronizer, string(s))
}

var (
	expectedSynchronizer = `// Code generated by kafmesh-gen. DO NOT EDIT.

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

	testId "test/internal/kafmesh/models/testMesh/testId"
)

type TestId_Test_View struct {
	view *goka.View
}

func New_TestId_Test_View(options runner.ServiceOptions) (*TestId_Test_View, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("testMesh.testId.test", &testId.Test{})
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

		path := filepath.Join("/tmp/storage", "view", "testMesh.testId.test")

		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create view db directory")
		}

		builder = storage.BuilderWithOptions(path, opts)
	}

	view, err := goka.NewView(g.settings.Brokers,
		goka.Table("testMesh.testId.test"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating view")
	}

	return &TestId_Test_View{
		view: view,
	}, nil
}

func (v *TestId_Test_View) Watch(ctx context.Context) func() error {
	return v.view.Run(ctx)
}

func (v *TestId_Test_View) Keys() []string {
	return v.Keys()
}

func (v *TestId_Test_View) Get(key string) (*testId.Test, error) {
	m, err := v.view.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get value from view")
	}

	msg, ok := m.(*testId.Test)
	if !ok {
		return nil, errors.Errorf("expecting message of type '*testId.Test' got type '%t'", m)
	}

	return msg, nil
}
`
)
