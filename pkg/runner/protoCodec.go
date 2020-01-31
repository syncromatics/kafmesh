package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	v1 "github.com/syncromatics/proto-schema-registry/pkg/proto/schema/registry/v1"
	"github.com/syncromatics/proto-schema-registry/pkg/protobuf"
	"google.golang.org/grpc"
)

const magicByte byte = 2

// ProtoWrapper is a codec generator for proto schema codecs
type ProtoWrapper struct {
	client *Registry
}

// NewProtoWrapper creates a new proto schema codec ProtoWrapper
func NewProtoWrapper(registry *Registry) *ProtoWrapper {
	return &ProtoWrapper{
		client: registry,
	}
}

// Codec is a goka codec for proto schema objects
type Codec struct {
	wrapper     *ProtoWrapper
	id          uint32
	constructor func() Message
}

// Message is a protobuf object
type Message interface {
	descriptor.Message
}

// Codec returns a codec for the protobuf object
func (w *ProtoWrapper) Codec(topic string, message Message) (*Codec, error) {
	t := reflect.ValueOf(message).Elem()
	constructor := func() Message {
		v := reflect.New(t.Type())
		return v.Interface().(Message)
	}
	schema, err := protobuf.ExtractSchema(constructor())
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract schema")
	}

	id, err := w.client.RegisterSchema(topic, schema)
	if err != nil {
		return nil, err
	}

	return &Codec{
		wrapper:     w,
		id:          id,
		constructor: constructor,
	}, nil
}

// Decode decodes the bytes into a proto object
func (w *Codec) Decode(data []byte) (interface{}, error) {
	obj := w.constructor()
	err := proto.Unmarshal(data[5:], obj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal bytes")
	}
	return obj, nil
}

// Encode encodes the proto object into bytes
func (w *Codec) Encode(message interface{}) ([]byte, error) {
	bytes := make([]byte, 5)
	bytes[0] = magicByte

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, w.id)

	for i, b := range idBytes {
		bytes[i+1] = b
	}

	m := message.(Message)
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal object")
	}
	return append(bytes, b...), nil
}

// Registry is the proto schema registry api
type Registry struct {
	client v1.RegistryAPIClient
}

// NewRegistry creates a new proto schema registry
func NewRegistry(url string) (*Registry, error) {
	con, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial server")
	}

	client := v1.NewRegistryAPIClient(con)
	return &Registry{client}, nil
}

// WaitForRegistryToBeReady waits for the register to start responding
func (r *Registry) WaitForRegistryToBeReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	var err error
	for time.Now().Before(deadline) {
		_, err := r.client.Ping(context.Background(), &v1.PingRequest{})
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return errors.Wrap(err, "timed out waiting for registry to become ready")
}

// RegisterSchema registers the proto schema with the proto schema registry
func (r *Registry) RegisterSchema(topic, schema string) (uint32, error) {
	var b bytes.Buffer
	err := gzipWrite(&b, []byte(schema))
	if err != nil {
		return 0, errors.Wrap(err, "failed to gzip schema")
	}

	resp, err := r.client.RegisterSchema(context.Background(), &v1.RegisterSchemaRequest{
		Topic:  topic,
		Schema: b.Bytes(),
	})
	if err != nil {
		return 0, errors.Wrap(err, "failed to register schema with registry")
	}

	switch v := resp.Response.(type) {
	case *v1.RegisterSchemaResponse_ResponseSuccess:
		return v.ResponseSuccess.Id, nil

	case *v1.RegisterSchemaResponse_ResponseError:
		errs := strings.Join(v.ResponseError.Errors, ", ")
		return 0, errors.Errorf("validation errors trying to register schema: %s", errs)

	default:
		return 0, errors.Errorf("unknown response type: %t", v)
	}
}

func gzipWrite(w io.Writer, data []byte) error {
	gw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	defer gw.Close()
	gw.Write(data)
	return err
}
