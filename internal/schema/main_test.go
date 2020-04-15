package schema_test

import (
	"path/filepath"
	"runtime"
)

func getPath() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filename)
}
