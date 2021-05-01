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
type {{ .Name }}_ViewSource_Context interface {
	Context() context.Context
	{{- with (eq .Type "protobuf") }}
	Update(string, *{{ $t.MessageType }}) error
	{{- end -}}
	{{- with (eq .Type "raw") }}
	Update(string, []byte) error
	{{- end }}
}

type {{ .Name }}_ViewSource interface {
	Sync({{ .Name }}_ViewSource_Context) error
}

type contextWrap_{{ .Name }} struct {
	context context.Context
	{{- with (eq .Type "protobuf") }}
	job *runner.ProtoViewSourceJob
	{{- end -}}
	{{- with (eq .Type "raw") }}
	job *runner.RawViewSourceJob
	{{- end }}
}

func (c *contextWrap_{{ $t.Name }}) Context() context.Context {
	return c.context
}

{{- with (eq .Type "protobuf") }}
func (c *contextWrap_{{ $t.Name }}) Update(key string, msg *{{ $t.MessageType }}) error {
{{- end -}}
{{- with (eq .Type "raw") }}
func (c *contextWrap_{{ $t.Name }}) Update(key string, msg []byte) error {
{{- end }}
	return c.job.Update(key, msg)
}

func Register_{{ .Name }}_ViewSource(options runner.ServiceOptions, synchronizer {{ .Name }}_ViewSource, updateInterval time.Duration, syncTimeout time.Duration) (func(context.Context) func() error, error) {
	brokers := options.Brokers
	var err error

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

	path := filepath.Join("/tmp/storage", "viewSource", "{{ .TopicName }}")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create view source db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)
	view, err := goka.NewView(brokers,
		goka.Table("{{ .TopicName }}"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed creating synchronizer view")
	}

	e, err := goka.NewEmitter(brokers,
		goka.Stream("{{ .TopicName }}"),
		codec,
		goka.WithEmitterHasher(kafkautil.MurmurHasher))

	if err != nil {
		return nil, errors.Wrap(err, "failed creating synchronizer emitter")
	}

	emitter := runner.NewEmitter(e)

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
						{{- with (eq .Type "protobuf") }}
						c := runner.NewProtoViewSourceJob(newContext, view, emitter)
						{{- end -}}
						{{- with (eq .Type "raw") }}
						c := runner.NewRawViewSourceJob(newContext, view, emitter)
						{{- end }}
						cw := &contextWrap_{{ .Name }}{newContext, c}
						err := synchronizer.Sync(cw)
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

			grp.Go(emitter.Watch(ctx))
			grp.Go(func() error {
				return view.Run(ctx)
			})

			select {
			case <- ctx.Done():
				err := grp.Wait()
				return err
			}
		}
	}, nil
}
