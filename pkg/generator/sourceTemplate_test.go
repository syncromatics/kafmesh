package generator_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateEmitter(tmpDir string, t *testing.T) {
	s, err := ioutil.ReadFile(path.Join(tmpDir, "internal", "kafmesh", "details", "testSerial_details_source.km.go"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedEmitter, string(s))
}

var (
	expectedEmitter = `// Code generated by kafmesh-gen. DO NOT EDIT.

package details

import (
	"context"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"

	"github.com/syncromatics/kafmesh/pkg/runner"

	"test/internal/kafmesh/models/testMesh/testSerial"
)

type TestSerialDetails_Source interface {
	Emit(message TestSerialDetails_Source_Message) error
	EmitBulk(ctx context.Context, messages []TestSerialDetails_Source_Message) error
	Delete(key string) error
}

type TestSerialDetails_Source_impl struct {
	emitter *runner.Emitter
}

type TestSerialDetails_Source_Message struct {
	Key string
	Value *testSerial.Details
}

type impl_TestSerialDetails_Source_Message struct {
	msg TestSerialDetails_Source_Message
}

func (m *impl_TestSerialDetails_Source_Message) Key() string {
	return m.msg.Key
}

func (m *impl_TestSerialDetails_Source_Message) Value() interface{} {
	return m.msg.Value
}

func New_TestSerialDetails_Source(options runner.ServiceOptions) (*TestSerialDetails_Source_impl, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("testMesh.testSerial.details", &testSerial.Details{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	emitter, err := goka.NewEmitter(brokers,
		goka.Stream("testMesh.testSerial.details"),
		codec,
		goka.WithEmitterHasher(kafkautil.MurmurHasher))

	if err != nil {
		return nil, errors.Wrap(err, "failed creating source")
	}

	return &TestSerialDetails_Source_impl{
		emitter: runner.NewEmitter(emitter),
	}, nil
}

func (e *TestSerialDetails_Source_impl) Watch(ctx context.Context) func() error {
	return e.emitter.Watch(ctx)
}

func (e *TestSerialDetails_Source_impl) Emit(message TestSerialDetails_Source_Message) error {
	return e.emitter.Emit(message.Key, message.Value)
}

func (e *TestSerialDetails_Source_impl) EmitBulk(ctx context.Context, messages []TestSerialDetails_Source_Message) error {
	b := []runner.EmitMessage{}
	for _, m := range messages {
		b = append(b, &impl_TestSerialDetails_Source_Message{msg: m})
	}
	return e.emitter.EmitBulk(ctx, b)
}

func (e *TestSerialDetails_Source_impl) Delete(key string) error {
	return e.emitter.Emit(key, nil)
}
`
)