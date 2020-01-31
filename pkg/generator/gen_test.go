package generator_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/syncromatics/kafmesh/pkg/generator"
	"github.com/syncromatics/kafmesh/pkg/models"
)

func Test_Generator(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "Test_Generator")
	if err != nil {
		t.Fatal(err)
	}
	//tmpDir := path.Join("/tmp", "genTest")

	protoDir := path.Join(tmpDir, "protos")
	err = os.MkdirAll(protoDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	package1 := path.Join(protoDir, "testMesh", "testId")
	err = os.MkdirAll(package1, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(package1, "test.proto"), []byte(`syntax ="proto3";
package testMesh.testId;

message Test {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package1, "test2.proto"), []byte(`syntax ="proto3";
package testMesh.testId;

message Test2 {
	string serial = 1;
}`), os.ModePerm)

	package2 := path.Join(protoDir, "testMesh", "testSerial")
	err = os.MkdirAll(package2, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(package2, "details.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message Details {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package2, "detailsState.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message DetailsState {
	string name = 1;
}`), os.ModePerm)

	ioutil.WriteFile(path.Join(package2, "detailsEnriched.proto"), []byte(`syntax ="proto3";
package testMesh.testSerial;

message DetailsEnriched {
	string name = 1;
}`), os.ModePerm)

	options := generator.Options{
		Service: &models.Service{
			Output: models.OutputSettings{
				Path:    "kafmesh",
				Package: "kafmesh",
			},
			Messages: models.MessageDefinitions{
				Protobuf: []string{
					"./protos",
				},
			},
		},
		RootPath: tmpDir,
		Components: []*models.Component{
			&models.Component{
				Name: "details",
				Processors: []models.Processor{
					models.Processor{
						GroupName: "testMesh.testId.test2",
						Inputs: []models.Input{
							models.Input{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testId.test",
								},
							},
							models.Input{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testId.test2",
								},
							},
						},
						Lookups: []models.Lookup{
							models.Lookup{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.details",
								},
							},
						},
						Joins: []models.Join{
							models.Join{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.details",
								},
							},
						},
						Outputs: []models.Output{
							models.Output{
								TopicDefinition: models.TopicDefinition{
									Message: "testMesh.testSerial.detailsEnriched",
								},
							},
						},
						Persistence: &models.Persistence{
							TopicDefinition: models.TopicDefinition{
								Message: "testMesh.testSerial.detailsState",
							},
						},
					},
				},
			},
		},
		Mod: "test",
	}

	err = generator.Generate(options)
	if err != nil {
		t.Fatal(err)
	}

	s, err := ioutil.ReadFile(path.Join(tmpDir, "kafmesh", "details", "testMesh_testId_test2.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedDetails, string(s))

	s, err = ioutil.ReadFile(path.Join(tmpDir, "kafmesh", "service.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedService, string(s))
}

var (
	expectedDetails = `// Code generated by kafmesh-gen. DO NOT EDIT.

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

	m0 "test/kafmesh/models/testMesh/testId"
	m1 "test/kafmesh/models/testMesh/testSerial"
)

type TestMeshTestIdTest2ProcessorContext interface {
	Lookup_TestSerialDetails(key string) *m1.Details
	Join_TestSerialDetails() *m1.Details
	Output_TestSerialDetailsEnriched(key string, message *m1.DetailsEnriched)
	SaveState(state *m1.DetailsState)
	State() *m1.DetailsState
}

type TestMeshTestIdTest2Processor interface {
	HandleInput_TestIdTest(ctx TestMeshTestIdTest2ProcessorContext, message *m0.Test) error
	HandleInput_TestIdTest2(ctx TestMeshTestIdTest2ProcessorContext, message *m0.Test2) error
}

type TestMeshTestIdTest2ProcessorContextImpl struct {
	ctx goka.Context
}

func newTestMeshTestIdTest2ProcessorContextImpl(ctx goka.Context) *TestMeshTestIdTest2ProcessorContextImpl {
	return &TestMeshTestIdTest2ProcessorContextImpl{ctx}
}

func (c *TestMeshTestIdTest2ProcessorContextImpl) Lookup_TestSerialDetails(key string) *m1.Details {
	v := c.ctx.Lookup("testMesh.testSerial.details", key)
	return v.(*m1.Details)
}

func (c *TestMeshTestIdTest2ProcessorContextImpl) Join_TestSerialDetails() *m1.Details {
	v := c.ctx.Join("testMesh.testSerial.details")
	return v.(*m1.Details)
}

func (c *TestMeshTestIdTest2ProcessorContextImpl) Output_TestSerialDetailsEnriched(key string, message *m1.DetailsEnriched) {
	c.ctx.Emit("testMesh.testSerial.detailsEnriched", key, message)
}

func (c *TestMeshTestIdTest2ProcessorContextImpl) SaveState(state *m1.DetailsState) {
	c.ctx.SetValue(state)
}

func (c *TestMeshTestIdTest2ProcessorContextImpl) State() *m1.DetailsState {
	v := c.ctx.Value()
	t := v.(*m1.DetailsState)
	return t
}


func Register_TestMeshTestIdTest2Processor(options runner.ServiceOptions, service TestMeshTestIdTest2Processor) (func(context.Context) func() error, error) {
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
			w := newTestMeshTestIdTest2ProcessorContextImpl(ctx)
			err := service.HandleInput_TestIdTest(w, msg)
			if err != nil {
				ctx.Fail(err)
			}
		}),
		goka.Input(goka.Stream("testMesh.testId.test2"), c1, func(ctx goka.Context, m interface{}) {
			msg := m.(*m0.Test2)
			w := newTestMeshTestIdTest2ProcessorContextImpl(ctx)
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

	expectedService = `// Code generated by kafmesh-gen. DO NOT EDIT.
package kafmesh

import (
	"github.com/syncromatics/kafmesh/pkg/runner"
	"github.com/pkg/errors"
	"test/kafmesh/details"
)
func Register_TestMeshTestIdTest2Processor(service *runner.Service, processor details.TestMeshTestIdTest2Processor) error {
	r, err := details.Register_TestMeshTestIdTest2Processor(service.Options(), processor)
	if err != nil {
		return errors.Wrap(err, "failed to register processor")
	}

	err = service.RegisterRunner(r)
	if err != nil {
		return errors.Wrap(err, "failed to register runner with service")
	}

	return nil
}
`
)
