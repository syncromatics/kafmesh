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
	"time"
	"fmt"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/sync/errgroup"

	"github.com/syncromatics/kafmesh/pkg/runner"

	"test/internal/kafmesh/models/testMesh/testId"
)

type TestToDatabase_Synchronizer_Context interface {
	context.Context
	Update(string, *testId.Test) error
}

type TestToDatabase_Synchronizer interface {
	Sync(TestToDatabase_Synchronizer_Context) error
}

type contextWrap_TestToDatabase struct {
	context.Context
	job *runner.ProtoSynchronizerJob
}

func (c *contextWrap_TestToDatabase) Update(key string, msg *testId.Test) error {
	return c.job.Update(key, msg)
}

func Register_TestToDatabase_Synchronizer(options runner.ServiceOptions, sychronizer TestToDatabase_Synchronizer, updateInterval time.Duration, syncTimeout time.Duration) (func(context.Context) func() error, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("testMesh.testId.test", &testId.Test{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "synchronizer", "testMesh.testId.test")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create synchronizer db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)

	view, err := goka.NewView(brokers,
		goka.Table("testMesh.testId.test"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating sychronizer view")
	}

	e, err := goka.NewEmitter(brokers,
		goka.Stream("testMesh.testId.test"),
		codec,
		goka.WithEmitterHasher(kafkautil.MurmurHasher))

	if err != nil {
		return nil, errors.Wrap(err, "failed creating sychronizer emitter")
	}

	emitter := runner.NewEmitter(e)

	return func(ctx context.Context) func() error {
		return func() error {
			gctx, cancel := context.WithCancel(ctx)
			grp, gctx := errgroup.WithContext(ctx)
			defer cancel()

			timer := time.NewTimer(0)
			grp.Go(func() error {
				for {
					select {
					case <-gctx.Done():
						return nil
					case <-timer.C:
						newContext, cancel := context.WithTimeout(gctx, syncTimeout)
						c := runner.NewProtoSynchronizerJob(newContext, view, emitter)
						cw := &contextWrap_TestToDatabase{newContext, c}
						err := sychronizer.Sync(cw)
						if err != nil {
							cancel()
							fmt.Printf("sync error '%v'", err)
							return err
						}
						err = c.Finish()
						if err != nil {
							cancel()
							fmt.Printf("sync finish error '%v'", err)
							return err
						}
						cancel()
						timer = time.NewTimer(updateInterval)
					}
				}
			})

			grp.Go(emitter.Watch(gctx))
			grp.Go(func() error {
				return view.Run(gctx)
			})

			select {
			case <- ctx.Done():
				return nil
			case <- gctx.Done():
				err := grp.Wait()
				return err
			}
		}
	}, nil
}
`
)
