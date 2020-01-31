package generator

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/pkg/models"
	"github.com/yargevad/filepathx"
)

// Options for the kafmesh generator
type Options struct {
	Service    *models.Service
	Components []*models.Component
	RootPath   string
	Mod        string
}

// Generate generates the kafmesh files
func Generate(options Options) error {
	outputPath := path.Join(options.RootPath, options.Service.Output.Path)

	err := os.MkdirAll(outputPath, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "failed to create output path")
	}

	includes := []string{}
	files := []string{}
	for _, p := range options.Service.Messages.Protobuf {
		protoPath := path.Join(options.RootPath, p)
		includes = append(includes, protoPath)

		fs, err := filepathx.Glob(path.Join(protoPath, "**/*.proto"))
		if err != nil {
			return errors.Wrap(err, "failed to glob files")
		}

		files = append(files, fs...)
	}

	args := append(includes, files...)

	modelsPath := path.Join(options.RootPath, options.Service.Output.Path, "models")
	err = os.MkdirAll(modelsPath, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "failed to create output models path")
	}

	args = append(args, "--go_out=.")

	err = Protoc(protoOptions{
		Files:    files,
		Includes: includes,
		Output:   modelsPath,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to run protoc")
	}

	file, err := os.Create(path.Join(outputPath, "service.km.go"))
	if err != nil {
		return errors.Wrapf(err, "failed to open service file")
	}
	defer file.Close()

	for _, c := range options.Components {
		err = processComponent(options.RootPath, outputPath, options.Mod, modelsPath, c)
		if err != nil {
			return errors.Wrapf(err, "failed to process component")
		}
	}

	sOptions, err := buildServiceOptions(options.Service, options.Components, options.Mod)
	if err != nil {
		return errors.Wrapf(err, "failed to build options")
	}

	err = generateService(file, sOptions)
	if err != nil {
		return errors.Wrapf(err, "failed to generate package")
	}

	return nil
}

func processComponent(rootPath string, outputPath string, mod string, modelsPath string, component *models.Component) error {
	mPath := strings.TrimPrefix(modelsPath, rootPath)
	componentPath := path.Join(outputPath, component.Name)
	_, err := os.Stat(componentPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(componentPath, os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "failed to create component path")
		}
	}

	for _, p := range component.Processors {
		fileName := strings.ReplaceAll(p.GroupName, ".", "_")
		fileName = fmt.Sprintf("%s_processor.km.go", fileName)
		file, err := os.Create(path.Join(componentPath, fileName))
		if err != nil {
			return errors.Wrapf(err, "failed to open service file")
		}
		defer file.Close()

		co, err := buildProcessorOptions(component.Name, mod, mPath, p)
		if err != nil {
			return errors.Wrap(err, "failed to build processor options")
		}

		err = generateProcessor(file, co)
		if err != nil {
			return errors.Wrap(err, "failed to generate processor")
		}
	}

	for _, e := range component.Emitters {
		fileName := strings.ReplaceAll(e.Message, ".", "_")
		fileName = fmt.Sprintf("%s_emitter.km.go", fileName)
		file, err := os.Create(path.Join(componentPath, fileName))
		if err != nil {
			return errors.Wrapf(err, "failed to open service file")
		}
		defer file.Close()

		co, err := buildEmitterOptions(component.Name, mod, mPath, e)
		if err != nil {
			return errors.Wrap(err, "failed to build emitter options")
		}

		err = generateEmitter(file, co)
		if err != nil {
			return errors.Wrap(err, "failed to generate emitter")
		}
	}

	return nil
}
