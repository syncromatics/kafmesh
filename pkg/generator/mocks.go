package generator

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"strings"

	"github.com/pkg/errors"
)

func generateMocks(fullOutputDir string) error {
	directories, err := ioutil.ReadDir(fullOutputDir)
	if err != nil {
		return errors.Wrap(err, "failed to get output directories")
	}

	for _, d := range directories {
		if !d.IsDir() {
			continue
		}

		if d.Name() == "models" {
			continue
		}

		full := path.Join(fullOutputDir, d.Name())

		files, err := ioutil.ReadDir(full)
		if err != nil {
			return errors.Wrapf(err, "failed to read files in '%s'", d.Name())
		}

		for _, f := range files {
			if strings.Contains(f.Name(), ".mock.go") {
				continue
			}

			name := strings.TrimRight(f.Name(), ".go")

			err = mockGen(path.Join(full, name+".go"), path.Join(full, name+".mock.go"), d.Name())
			if err != nil {
				return errors.Wrapf(err, "failed to gen mock for '%s", name)
			}

		}

	}
	return nil
}

func mockGen(source string, destination string, pkg string) error {
	args :=
		[]string{
			fmt.Sprintf("-source=%s", source),
			fmt.Sprintf("-destination=%s", destination),
			fmt.Sprintf("-package=%s", pkg),
		}

	errBuf := bytes.NewBuffer([]byte{})

	cmd := exec.Command("mockgen", args...)
	cmd.Stderr = errBuf

	_, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "failed to run mockgen: %s", string(errBuf.Bytes()))
	}

	if cmd.ProcessState.ExitCode() != 0 {
		return errors.Errorf("mockgen did not return 0")
	}

	return nil
}
