package generator

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
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
	gopath, ok := os.LookupEnv("GOPATH")
	if !ok {
		return errors.Errorf("no GOPATH defined")
	}

	goBin := path.Join(gopath, "bin")

	_, err := exec.LookPath(path.Join(goBin, "protoc-gen-go"))
	if err != nil {
		return errors.Wrap(err, "could not find protoc-gen-go")
	}

	pathenv, _ := os.LookupEnv("PATH")

	args := []string{}
	for _, i := range options.Includes {
		args = append(args, "-I")
		args = append(args, i)
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

		m = fmt.Sprintf("M%s=%s", strings.TrimLeft(r, "/"), m)

		modules = append(modules, m)
	}

	for _, files := range packages {
		pargs := append(args)
		for _, f := range files {
			pargs = append(pargs, f)
		}

		output := options.Output
		if runtime.GOOS == "windows" {
			output = strings.ReplaceAll(output, "/", "\\")
		}

		pargs = append(pargs, fmt.Sprintf("--go_out=%s,plugins=grpc:%s", strings.Join(modules, ","), output))

		errBuf := bytes.NewBuffer([]byte{})
		cmd := exec.Command("protoc", pargs...)
		cmd.Env = []string{
			fmt.Sprintf("PATH=%s:%s:", pathenv, goBin),
		}
		cmd.Stderr = errBuf

		_, err = cmd.Output()
		if err != nil {
			return errors.Wrapf(err, "failed to run protoc: %s", string(errBuf.Bytes()))
		}

		if cmd.ProcessState.ExitCode() != 0 {
			return errors.Errorf("protoc did not return 0")
		}
	}

	return nil
}
