package {{ .Package }}

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

	{{ .Import }}
)

{{ $t := . -}}
type {{ .Name }}_ViewSink_Context interface {
	Context() context.Context
	Keys() ([]string, error)
	{{- with (eq .Type "protobuf") }}
	Get(string) (*{{ $t.MessageType }}, error)
	{{- end -}}
	{{- with (eq .Type "raw") }}
	Get(string) ([]byte, error)
	{{- end }}
}

type {{ .Name }}_ViewSink_Context_impl struct {
	context context.Context
	view    *goka.View
}

func (c *{{ .Name }}_ViewSink_Context_impl) Context() context.Context {
	return c.context
}

func (c *{{ .Name }}_ViewSink_Context_impl) Keys() ([]string, error) {
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

{{ with (eq .Type "protobuf") -}}
func (c *{{ $t.Name }}_ViewSink_Context_impl) Get(key string) (*{{ $t.MessageType }}, error) {
{{- end -}}
{{- with (eq .Type "raw") -}}
func (c *{{ $t.Name }}_ViewSink_Context_impl) Get(key string) ([]byte, error) {
{{- end }}
	m, err := c.view.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get value from view")
	}
	if m == nil {
		return nil, nil
	}
	{{- with (eq .Type "protobuf") }}
	msg, ok := m.(*{{ $t.MessageType }})
	{{- end -}}
	{{- with (eq .Type "raw") }}
	msg, ok := m.([]byte)
	{{- end }}
	if !ok {
		{{- with (eq .Type "protobuf") }}
		return nil, errors.Errorf("expecting message of type '*{{ $t.MessageType }}' got type '%t'", m)
		{{- end -}}
		{{- with (eq .Type "raw") }}
		return nil, errors.Errorf("expecting message of type '[]byte' got type '%t'", m)
		{{- end }}
	}
	return msg, nil
}

type {{ .Name }}_ViewSink interface {
	Sync({{ .Name }}_ViewSink_Context) error
}

func Register_{{ .Name }}_ViewSink(options runner.ServiceOptions, synchronizer {{ .Name }}_ViewSink, updateInterval time.Duration, syncTimeout time.Duration) (func(context.Context) func() error, error) {
	brokers := options.Brokers

	{{- with (eq .Type "protobuf") }}
	protoWrapper := options.ProtoWrapper
	codec, err := protoWrapper.Codec("{{ $t.TopicName }}", &{{ $t.MessageType }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	codec := &gokaCodecs.Bytes{}
	{{- end }}

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "viewSink", "{{ .TopicName }}")

	{{- with (eq .Type "protobuf") }}
	err = os.MkdirAll(path, os.ModePerm)
	{{- end -}}
	{{- with (eq .Type "raw") }}
	err := os.MkdirAll(path, os.ModePerm)
	{{- end }}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create view sink db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)
	view, err := goka.NewView(brokers,
		goka.Table("{{ .TopicName }}"),
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
						c := &{{ .Name }}_ViewSink_Context_impl{
							context: newContext,
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
