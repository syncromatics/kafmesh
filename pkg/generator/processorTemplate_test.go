package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateProcessors(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testMesh_testId_test2_processor.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedDetailsProcessor, string(s))
}

var (
	expectedDetailsProcessor = `// Code generated by kafmesh-gen. DO NOT EDIT.

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

	m0 "test/internal/kafmesh/models/testMesh/testId"
	m1 "test/internal/kafmesh/models/testMesh/testSerial"
)

type TestMeshTestIdTest2_ProcessorContext interface {
	Lookup_TestSerialDetails(key string) *m1.Details
	Join_TestSerialDetails() *m1.Details
	Output_TestSerialDetailsEnriched(key string, message *m1.DetailsEnriched)
	SaveState(state *m1.DetailsState)
	State() *m1.DetailsState
}

type TestMeshTestIdTest2_Processor interface {
	HandleInput_TestIdTest(ctx TestMeshTestIdTest2_ProcessorContext, message *m0.Test) error
	HandleInput_TestIdTest2(ctx TestMeshTestIdTest2_ProcessorContext, message *m0.Test2) error
}

type TestMeshTestIdTest2_ProcessorContext_Impl struct {
	ctx goka.Context
}

func new_TestMeshTestIdTest2_ProcessorContext_Impl(ctx goka.Context) *TestMeshTestIdTest2_ProcessorContext_Impl {
	return &TestMeshTestIdTest2_ProcessorContext_Impl{ctx}
}

func (c *TestMeshTestIdTest2_ProcessorContext_Impl) Lookup_TestSerialDetails(key string) *m1.Details {
	v := c.ctx.Lookup("testMesh.testSerial.details", key)
	return v.(*m1.Details)
}

func (c *TestMeshTestIdTest2_ProcessorContext_Impl) Join_TestSerialDetails() *m1.Details {
	v := c.ctx.Join("testMesh.testSerial.details")
	return v.(*m1.Details)
}

func (c *TestMeshTestIdTest2_ProcessorContext_Impl) Output_TestSerialDetailsEnriched(key string, message *m1.DetailsEnriched) {
	c.ctx.Emit("testMesh.testSerial.detailsEnriched", key, message)
}

func (c *TestMeshTestIdTest2_ProcessorContext_Impl) SaveState(state *m1.DetailsState) {
	c.ctx.SetValue(state)
}

func (c *TestMeshTestIdTest2_ProcessorContext_Impl) State() *m1.DetailsState {
	v := c.ctx.Value()
	t := v.(*m1.DetailsState)
	return t
}

func Register_TestMeshTestIdTest2_Processor(options runner.ServiceOptions, service TestMeshTestIdTest2_Processor) (func(context.Context) func() error, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	config := kafka.NewConfig()
	config.Consumer.Offsets.Initial = kafka.OffsetOldest

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	builder := storage.BuilderWithOptions("/tmp/storage", opts)

	c0, err := protoWrapper.Codec("testMesh.testId.test", &m0.Test{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	c1, err := protoWrapper.Codec("testMesh.testId.test2", &m0.Test2{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	c2, err := protoWrapper.Codec("testMesh.testSerial.details", &m1.Details{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	c3, err := protoWrapper.Codec("testMesh.testSerial.detailsEnriched", &m1.DetailsEnriched{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	c4, err := protoWrapper.Codec("testMesh.testId.test2-table", &m1.DetailsState{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	edges := []goka.Edge{
		goka.Input(goka.Stream("testMesh.testId.test"), c0, func(ctx goka.Context, m interface{}) {
			msg := m.(*m0.Test)
			w := new_TestMeshTestIdTest2_ProcessorContext_Impl(ctx)
			err := service.HandleInput_TestIdTest(w, msg)
			if err != nil {
				ctx.Fail(err)
			}
		}),
		goka.Input(goka.Stream("testMesh.testId.test2"), c1, func(ctx goka.Context, m interface{}) {
			msg := m.(*m0.Test2)
			w := new_TestMeshTestIdTest2_ProcessorContext_Impl(ctx)
			err := service.HandleInput_TestIdTest2(w, msg)
			if err != nil {
				ctx.Fail(err)
			}
		}),
		goka.Lookup(goka.Table("testMesh.testSerial.details"), c2),
		goka.Join(goka.Table("testMesh.testSerial.details"), c2),
		goka.Output(goka.Stream("testMesh.testSerial.detailsEnriched"), c3),
		goka.Persist(c4),
	}
	group := goka.DefineGroup(goka.Group("testMesh.testId.test2"), edges...)

	processor, err := goka.NewProcessor(brokers,
		group,
		goka.WithConsumerBuilder(kafka.ConsumerBuilderWithConfig(config)),
		goka.WithStorageBuilder(builder),
		goka.WithHasher(kafkautil.MurmurHasher))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create goka processor")
	}

	return func(ctx context.Context) func() error {
		return func() error {
			err := processor.Run(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to run goka processor")
			}

			return nil
		}
	}, nil
}
`
)
