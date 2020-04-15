package testing

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/burdiyan/kafkautil"
	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"
	protobufD "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/lovoo/goka"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/runner"
)

// Emitter is used to emit messages into kafka for testing
type Emitter struct {
	wrapper *runner.ProtoWrapper
	brokers []string
}

//NewEmitter creates a new test emitter
func NewEmitter(brokers []string, registry *runner.Registry) *Emitter {
	wrapper := runner.NewProtoWrapper(registry)
	return &Emitter{wrapper, brokers}
}

// Emit a message to kafka using the topic extracted from the name
func (e *Emitter) Emit(key string, message runner.Message) error {
	topic, err := extractTopic(message)
	if err != nil {
		return errors.Wrap(err, "failed to extract topic from message")
	}

	codec, err := e.wrapper.Codec(topic, message)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	emitter, err := goka.NewEmitter(e.brokers, goka.Stream(topic), codec, goka.WithEmitterHasher(kafkautil.MurmurHasher))
	if err != nil {
		return errors.Wrap(err, "failed to create emitter")
	}
	defer emitter.Finish()

	err = emitter.EmitSync(key, message)
	if err != nil {
		return errors.Wrap(err, "failed to emit message")
	}

	return nil
}

// EmitToTopic emits a message to kafka
func (e *Emitter) EmitToTopic(topic string, key string, message runner.Message) error {
	codec, err := e.wrapper.Codec(topic, message)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	emitter, err := goka.NewEmitter(e.brokers, goka.Stream(topic), codec, goka.WithEmitterHasher(kafkautil.MurmurHasher))
	if err != nil {
		return errors.Wrap(err, "failed to create emitter")
	}
	defer emitter.Finish()

	err = emitter.EmitSync(key, message)
	if err != nil {
		return errors.Wrap(err, "failed to emit message")
	}

	return nil
}

func extractTopic(message descriptor.Message) (string, error) {
	messageType := reflect.TypeOf(message).Elem().Name()

	d, _ := message.Descriptor()
	desc, err := extractDescriptor(d)
	if err != nil {
		return "", errors.Wrap(err, "failed to extract proto descriptor")
	}

	messageType = firstToLower(messageType)

	return fmt.Sprintf("%s.%s", *desc.Package, messageType), nil
}

func extractDescriptor(m []byte) (*protobufD.FileDescriptorProto, error) {
	r, err := gzip.NewReader(bytes.NewReader(m))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create gzip ready")
	}
	defer r.Close()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read from gzip")
	}

	desc := &protobufD.FileDescriptorProto{}
	err = proto.Unmarshal(buf, desc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal proto")
	}

	return desc, nil
}

func firstToLower(s string) string {
	if len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r != utf8.RuneError || size > 1 {
			lo := unicode.ToLower(r)
			if lo != r {
				s = string(lo) + s[size:]
			}
		}
	}
	return s
}
