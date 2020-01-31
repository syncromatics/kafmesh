package schema

import (
	"fmt"
	"os"
	"path"
	"unicode"
	"unicode/utf8"

	"github.com/emicklei/proto"
	"github.com/pkg/errors"
	"github.com/yargevad/filepathx"
)

// DescribeProtobufSchema extracts the messages in the proto path
func DescribeProtobufSchema(root string) (map[string]struct{}, error) {
	_, err := os.Stat(root)
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat root")
	}
	g := path.Join(root, "**/*.proto")

	messages := map[string]struct{}{}
	files, err := filepathx.Glob(g)
	if err != nil {
		return nil, errors.Wrap(err, "failed to glob")
	}

	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			return nil, errors.Wrap(err, "failed opening file")
		}

		pf := proto.NewParser(file)
		p, err := pf.Parse()
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse proto file")
		}

		var pkg *string
		ms := []string{}
		proto.Walk(p,
			proto.WithMessage(func(m *proto.Message) {
				ms = append(ms, firstToLower(m.Name))
			}),
			proto.WithPackage(func(p *proto.Package) {
				pkg = &p.Name
			}))

		if pkg == nil {
			return nil, errors.Errorf("proto file '%s' did not have package defined", f)
		}

		for _, m := range ms {
			messages[fmt.Sprintf("%s.%s", *pkg, m)] = struct{}{}
		}
	}

	return messages, nil
}

func firstToLower(s string) string {
	if len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r != utf8.RuneError || size > 1 {
			lo := unicode.ToLower(r)
			if lo != r {
				s = string(lo) + s[size:]
			}
		}
	}
	return s
}
