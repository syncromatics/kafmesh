package {{ .Package }}

import (
	"context"
	"os"
	"path/filepath"

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
type {{ .Name }}_View interface {
	Keys() []string
	{{- with (eq .Type "protobuf") }}
	Get(key string) (*{{ $t.MessageType }}, error)
	{{- end -}}
	{{- with (eq .Type "raw") }}
	Get(key string) ([]byte, error)
	{{- end }}
}

type {{ .Name }}_View_impl struct {
	context.Context
	view *goka.View
}

func New_{{ .Name }}_View(options runner.ServiceOptions) (*{{ .Name }}_View_impl, func(context.Context) func() error, error) {
	brokers := options.Brokers
	var err error

	{{- with (eq .Type "protobuf") }}
	protoWrapper := options.ProtoWrapper
	codec, err := protoWrapper.Codec("{{ $t.TopicName }}", &{{ $t.MessageType }}{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create codec")
	}
	{{- end -}}
	{{- with (eq .Type "raw") }}
	codec := &gokaCodecs.Bytes{}
	{{- end }}

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "view", "{{ .TopicName }}")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create view db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)

	view, err := goka.NewView(brokers,
		goka.Table("{{ .TopicName }}"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed creating view")
	}
	
	viewCtx, viewCancel := context.WithCancel(context.Background())
	v := &{{ .Name }}_View_impl{
		viewCtx,
		view,
	}

	return v, func(outerCtx context.Context) func() error {
		return func() error {
			cancelableCtx, cancel := context.WithCancel(outerCtx)
			defer cancel()
			grp, ctx := errgroup.WithContext(cancelableCtx)

			grp.Go(func() error {
				select {
				case <-ctx.Done():
					viewCancel()
					return nil
				}
			})
			grp.Go(func() error {
				return v.view.Run(ctx)
			})
			
			select {
			case <- ctx.Done():
				err := grp.Wait()
				return err
			}
		}
	}, nil
}

func (v *{{ .Name }}_View_impl) Keys() ([]string, error) {
	select {
	case <-v.Done():
		return nil, errors.New("context cancelled while waiting for partition to become running")
	case <-v.view.WaitRunning():
	}

	it, err := v.view.Iterator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get iterator from view")
	}
	
	keys := []string{}
	for it.Next() {
		keys = append(keys, it.Key())
	}

	return keys, nil
}

{{- with (eq .Type "protobuf") }}
func (v *{{ $t.Name }}_View_impl) Get(key string) (*{{ $t.MessageType }}, error) {
{{- end -}}
{{- with (eq .Type "raw") }}
func (v *{{ $t.Name }}_View_impl) Get(key string) ([]byte, error) {
{{- end }}
	select {
	case <-v.Done():
		return nil, errors.New("context cancelled while waiting for partition to become running")
	case <-v.view.WaitRunning():
	}

	m, err := v.view.Get(key)
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
