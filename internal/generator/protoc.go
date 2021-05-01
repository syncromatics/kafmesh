package generator

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type protoOptions struct {
	Includes []string
	Files    []file
	Output   string
	Module   string
	Path     string
}

type file struct {
	root string
	path string
}

// Protoc is a wrapper around the protec executable
func Protoc(options protoOptions) error {
	args := []string{}
	for _, i := range options.Includes {
		args = append(args, fmt.Sprintf("--proto_path=%s", i))
	}

	packages := map[string][]string{}
	modules := []string{}

	for _, f := range options.Files {
		p := filepath.Dir(f.path)
		_, ok := packages[p]
		if !ok {
			packages[p] = []string{}
		}
		packages[p] = append(packages[p], f.path)

		r := strings.ReplaceAll(f.path, f.root, "")
		r = strings.TrimLeft(r, "\\")
		r = strings.TrimLeft(r, "/")

		m := filepath.Dir(r)
		m = fmt.Sprintf("%s/%s/models/%s", options.Module, options.Path, m)
		m = strings.ReplaceAll(m, "\\", "/")

		r = strings.ReplaceAll(r, "\\", "/")

		m = fmt.Sprintf("--go_opt=M%s=%s", strings.TrimLeft(r, "/"), m)

		modules = append(modules, m)
	}

	for _, files := range packages {
		tmpDir, err := ioutil.TempDir("", "protocop")
		if err != nil {
			return errors.Wrap(err, "failed to get temp directory")
		}

		u2 := uuid.NewV4()
		configFile := path.Join(tmpDir, u2.String())

		output := options.Output
		if runtime.GOOS == "windows" {
			output = strings.ReplaceAll(output, "/", "\\")
		}

		pargs := append(args, fmt.Sprintf("--go_out=paths=source_relative:%s", output))
		pargs = append(pargs, modules...)
		pargs = append(pargs, files...)

		contents := strings.Join(pargs, "\n")
		err = ioutil.WriteFile(configFile, []byte(contents), os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "failed to create config file")
		}

		errBuf := bytes.NewBuffer([]byte{})
		cmd := exec.Command("protoc", "@"+configFile)
		cmd.Env = os.Environ()
		cmd.Stderr = errBuf

		_, err = cmd.Output()
		if err != nil {
			return errors.Wrapf(err, "failed to run protoc: %s", errBuf.String())
		}

		if cmd.ProcessState.ExitCode() != 0 {
			return errors.Errorf("protoc did not return 0")
		}
	}

	return nil
}
