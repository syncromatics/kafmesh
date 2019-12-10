package generator

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

type protoOptions struct {
	Includes []string
	Files    []string
	Output   string
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
	for _, f := range options.Files {
		p := filepath.Dir(f)
		_, ok := packages[p]
		if !ok {
			packages[p] = []string{}
		}
		packages[p] = append(packages[p], f)
	}

	for _, files := range packages {
		pargs := append(args)
		for _, f := range files {
			pargs = append(pargs, f)
		}
		pargs = append(pargs, "--go_out="+options.Output)

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
	}

	return nil
}
