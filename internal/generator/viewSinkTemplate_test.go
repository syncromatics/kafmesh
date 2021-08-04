package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateViewSink(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testId_test_viewSink.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedViewSink, string(s))
}

var (
	expectedViewSink = `// Code generated by kafmesh-gen. DO NOT EDIT.

package details

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/sync/errgroup"

	"test/internal/kafmesh/models/testMesh/testId"
)

type TestToApi_ViewSink_Context interface {
	context.Context
	Keys() ([]string, error)
	Get(string) (*testId.Test, error)
}

type TestToApi_ViewSink_Context_impl struct {
	context.Context
	view *goka.View
}

func (c *TestToApi_ViewSink_Context_impl) Keys() ([]string, error) {
	select {
	case <-c.Done():
		return nil, errors.New("context cancelled while waiting for partition to become running")
	case <-c.view.WaitRunning():
	}

	it, err := c.view.Iterator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get iterator")
	}
	keys := []string{}
	for it.Next() {
		keys = append(keys, it.Key())
	}
	return keys, nil
}

func (c *TestToApi_ViewSink_Context_impl) Get(key string) (*testId.Test, error) {
	select {
	case <-c.Done():
		return nil, errors.New("context cancelled while waiting for partition to become running")
	case <-c.view.WaitRunning():
	}

	m, err := c.view.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get value from view")
	}
	if m == nil {
		return nil, nil
	}
	msg, ok := m.(*testId.Test)
	if !ok {
		return nil, errors.Errorf("expecting message of type '*testId.Test' got type '%t'", m)
	}
	return msg, nil
}

type TestToApi_ViewSink interface {
	Sync(TestToApi_ViewSink_Context) error
}

func Register_TestToApi_ViewSink(options runner.ServiceOptions, synchronizer TestToApi_ViewSink, updateInterval time.Duration, syncTimeout time.Duration) (func(context.Context) func() error, error) {
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

	path := filepath.Join("/tmp/storage", "viewSink", "testMesh.testId.test")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create view sink db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)
	view, err := goka.NewView(brokers,
		goka.Table("testMesh.testId.test"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed creating view sink view")
	}

	return func(outerCtx context.Context) func() error {
		return func() error {
			cancelableCtx, cancel := context.WithCancel(outerCtx)
			defer cancel()
			grp, ctx := errgroup.WithContext(cancelableCtx)

			timer := time.NewTimer(0)
			grp.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-timer.C:
						select {
						case <-ctx.Done():
							return nil
						case <-view.WaitRunning():
						}
			
						newContext, cancel := context.WithTimeout(ctx, syncTimeout)
						c := &TestToApi_ViewSink_Context_impl{
							Context: newContext,
							view:    view,
						}
						err := synchronizer.Sync(c)
						if err != nil {
							cancel()
							fmt.Printf("sync error '%v'", err)
							return err
						}
						cancel()
						timer = time.NewTimer(updateInterval)
					}
				}
			})

			grp.Go(func() error {
				return view.Run(ctx)
			})

			select {
			case <- ctx.Done():
				return nil
			case <- ctx.Done():
				err := grp.Wait()
				return err
			}
		}
	}, nil
}
`
)
