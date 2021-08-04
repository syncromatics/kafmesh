package generator

import (
	"io"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/pkg/errors"
)

var (
	viewTemplate = template.Must(template.New("").Parse(`// Code generated by kafmesh-gen. DO NOT EDIT.

package {{ .Package }}

import (
	"context"
	"os"
	"path/filepath"

	"github.com/burdiyan/kafkautil"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/syncromatics/kafmesh/pkg/runner"

	"{{ .Import }}"
)

type {{ .Name }}_View interface {
	Keys() ([]string, error)
	Get(key string) (*{{ .MessageType }}, error)
}

type {{ .Name }}_View_impl struct {
	context.Context
	view *goka.View
}

func New_{{ .Name }}_View(options runner.ServiceOptions) (*{{ .Name }}_View_impl, func(context.Context) func() error, error) {
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

	path := filepath.Join("/tmp/storage", "view", "{{ .TopicName }}")

	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create view db directory")
	}

	builder := storage.BuilderWithOptions(path, opts)

	view, err := goka.NewView(brokers,
		goka.Table("{{ .TopicName }}"),
		codec,
		goka.WithViewStorageBuilder(builder),
		goka.WithViewHasher(kafkautil.MurmurHasher),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating view")
	}
	
	viewCtx, viewCancel := context.WithCancel(context.Background())
	v := &{{ .Name }}_View_impl{
		viewCtx,
		view,
	}

	return v, func(outerCtx context.Context) func() error {
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

func (v *{{ .Name }}_View_impl) Get(key string) (*{{ .MessageType }}, error) {
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

	msg, ok := m.(*{{ .MessageType }})
	if !ok {
		return nil, errors.Errorf("expecting message of type '*{{ .MessageType }}' got type '%t'", m)
	}

	return msg, nil
}
`))
)

type viewOptions struct {
	Package     string
	Import      string
	Name        string
	TopicName   string
	MessageType string
}

func generateView(writer io.Writer, view *viewOptions) error {
	err := viewTemplate.Execute(writer, view)
	if err != nil {
		return errors.Wrap(err, "failed to execute view template")
	}
	return nil
}

func buildViewOptions(pkg string, mod string, modelsPath string, service *models.Service, view models.View) (*viewOptions, error) {
	options := &viewOptions{
		Package: pkg,
	}

	options.TopicName = view.ToTopicName(service)
	options.Name = view.ToSafeMessageTypeName()
	options.Import = view.ToPackage(service)
	options.MessageType = view.ToMessageTypeWithPackage()

	return options, nil
}
