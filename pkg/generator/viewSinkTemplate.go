package generator

import (
	"io"
	"strings"
	"text/template"

	"github.com/iancoleman/strcase"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/models"
)

var (
	viewSinkTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

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

	"{{ .Import }}"
)

type {{ .Name }}_ViewSink_Context interface {
	context.Context
	Keys() ([]string, error)
	Get(string) (*{{ .MessageType }}, error)
}

type {{ .Name }}_ViewSink_Context_impl struct {
	context.Context
	view *goka.View
}

func (c *{{ .Name }}_ViewSink_Context_impl) Keys() ([]string, error) {
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

func (c *{{ .Name }}_ViewSink_Context_impl) Get(key string) (*{{ .MessageType }}, error) {
	m, err := c.view.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get value from view")
	}
	if m == nil {
		return nil, nil
	}
	msg, ok := m.(*{{ .MessageType }})
	if !ok {
		return nil, errors.Errorf("expecting message of type '*{{ .MessageType }}' got type '%t'", m)
	}
	return msg, nil
}

type {{ .Name }}_ViewSink interface {
	Sync({{ .Name }}_ViewSink_Context) error
}

func Register_{{ .Name }}_ViewSink(options runner.ServiceOptions, sychronizer {{ .Name }}_ViewSink, updateInterval time.Duration, syncTimeout time.Duration) (func(context.Context) func() error, error) {
	brokers := options.Brokers
	protoWrapper := options.ProtoWrapper

	codec, err := protoWrapper.Codec("{{ .TopicName }}", &{{ .MessageType }}{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	opts := &opt.Options{
		BlockCacheCapacity: opt.MiB * 1,
		WriteBuffer:        opt.MiB * 1,
	}

	path := filepath.Join("/tmp/storage", "viewSink", "{{ .TopicName }}")

	err = os.MkdirAll(path, os.ModePerm)
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
						c := &{{ .Name }}_ViewSync_Context_impl{
							Context: newContext,
							view:    view,
						}
						err := sychronizer.Sync(c)
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
`))
)

type viewSinkOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
}

func generateViewSink(writer io.Writer, viewSink *viewSinkOptions) error {
	err := viewSinkTemplate.Execute(writer, viewSink)
	if err != nil {
		return errors.Wrap(err, "failed to execute viewSink template")
	}
	return nil
}

func buildViewSinkOptions(pkg string, mod string, modelsPath string, service *models.Service, viewSink models.ViewSink) (*viewSinkOptions, error) {
	options := &viewSinkOptions{
		Package: pkg,
		Name:    viewSink.ToSafeName(),
	}

	var name strings.Builder
	nameFrags := strings.Split(viewSink.Message, ".")
	for _, f := range nameFrags[1:] {
		name.WriteString(strcase.ToCamel(f))
	}

	options.TopicName = viewSink.ToTopicName(service)
	options.Import = viewSink.ToPackage(service)
	options.MessageType = nameFrags[len(nameFrags)-2] + "." + strcase.ToCamel(nameFrags[len(nameFrags)-1])

	return options, nil
}
