package generator

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"github.com/syncromatics/kafmesh/internal/generator/templates"
	"github.com/syncromatics/kafmesh/internal/models"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
)

var (
	processorTemplate = template.Must(template.New("").Parse(templates.Processor))
)

type edge struct {
	Type            string
	Topic           string
	Message         string
	MessageType     string
	Codec           int
	Func            string
	RequiresPointer bool
}

type processorInterface struct {
	Name    string
	Methods []interfaceMethod
}

type interfaceMethod struct {
	Name string
	Args string
}

type contextMethod struct {
	interfaceMethod
	Type            string
	Topic           string
	MessageType     string
	MessageTypeName string
	RequiresPointer bool
}

type processorContext struct {
	Name    string
	Methods []contextMethod
}

type codec struct {
	Index   int
	Message string
	Topic   string
	Type    string
}

type processorOptions struct {
	Package       string
	Component     string
	ProcessorName string
	Context       processorContext
	Interface     processorInterface
	Imports       []string
	Group         string
	Edges         []edge
	Codecs        []codec
	Processor     models.Processor
}

func generateProcessor(writer io.Writer, processor *processorOptions) error {
	err := processorTemplate.Execute(writer, processor)
	if err != nil {
		return errors.Wrap(err, "failed to execute processor template")
	}
	return nil
}

func buildProcessorOptions(pkg string, mod string, modelsPath string, service *models.Service, component *models.Component, processor models.Processor) (*processorOptions, error) {
	imports := map[string]int{}
	importIndex := 0

	codecs := map[string]codec{}
	codecIndex := 0

	options := processorOptions{
		Package:       pkg,
		Component:     component.Name,
		ProcessorName: processor.Name,
		Imports:       []string{},
		Group:         processor.GroupName(service, component),
		Edges:         []edge{},
		Codecs:        []codec{},
		Processor:     models.Processor{},
	}

	options.Context = processorContext{
		Name:    processor.ToSafeName(),
		Methods: []contextMethod{},
	}

	intr := processorInterface{
		Name:    processor.ToSafeName(),
		Methods: []interfaceMethod{},
	}

	addGokaCodecs := false

	for _, input := range processor.Inputs {
		var name strings.Builder
		name.WriteString("Handle")
		nameFrags := strings.Split(input.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		topicType := "protobuf"
		if input.TopicDefinition.Type != nil {
			switch *input.TopicDefinition.Type {
			case "raw":
				topicType = "raw"
			}
		}

		var args strings.Builder
		args.WriteString(fmt.Sprintf("ctx %s_ProcessorContext", options.Context.Name))
		message := nameFrags[len(nameFrags)-1]

		fullMessageTypeName := ""
		requiresPointer := false
		switch topicType {
		case "protobuf":
			var mPkg strings.Builder
			for _, p := range nameFrags[:len(nameFrags)-1] {
				mPkg.WriteString("/")
				mPkg.WriteString(p)
			}

			modulePackage := input.ToPackage(service)

			i, ok := imports[modulePackage]
			if !ok {
				imports[modulePackage] = importIndex
				i = importIndex

				importIndex++
			}

			fullMessageTypeName = fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message))
			args.WriteString(fmt.Sprintf(", message *%s", fullMessageTypeName))
			requiresPointer = true
		case "raw":
			addGokaCodecs = true
			fullMessageTypeName = "[]byte"
			args.WriteString(", message []byte")
		}

		method := interfaceMethod{
			Name: fmt.Sprintf("Handle%s", input.ToSafeMessageTypeName()),
			Args: args.String(),
		}
		intr.Methods = append(intr.Methods, method)

		topic := input.ToTopicName(service)
		c, ok := codecs[topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   topic,
				Message: fullMessageTypeName,
				Type:    topicType,
			}
			codecs[topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:            "input",
			Topic:           topic,
			Message:         fullMessageTypeName,
			MessageType:     input.Message,
			Codec:           c.Index,
			Func:            method.Name,
			RequiresPointer: requiresPointer,
		})
	}
	options.Interface = intr

	for _, lookup := range processor.Lookups {
		var name strings.Builder
		name.WriteString("Lookup_")

		nameFrags := strings.Split(lookup.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		topicType := "protobuf"
		if lookup.TopicDefinition.Type != nil {
			switch *lookup.TopicDefinition.Type {
			case "raw":
				topicType = "raw"
			}
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]

		fullMessageTypeName := ""
		requiresPointer := false
		switch topicType {
		case "protobuf":
			requiresPointer = true

			var mPkg strings.Builder
			for _, p := range nameFrags[:len(nameFrags)-1] {
				mPkg.WriteString("/")
				mPkg.WriteString(p)
			}

			modulePackage := lookup.ToPackage(service)

			i, ok := imports[modulePackage]
			if !ok {
				imports[modulePackage] = importIndex
				i = importIndex

				importIndex++
			}
			fullMessageTypeName = fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message))
			args.WriteString(fmt.Sprintf("key string) *%s", fullMessageTypeName))
		case "raw":
			addGokaCodecs = true
			fullMessageTypeName = "[]byte"
			args.WriteString("key string) []byte")
		}

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Lookup_%s", lookup.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "lookup",
			MessageType:     fullMessageTypeName,
			MessageTypeName: lookup.Message,
			RequiresPointer: requiresPointer,
		}

		m.Topic = lookup.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fullMessageTypeName,
				Type:    topicType,
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:            "lookup",
			Codec:           c.Index,
			Topic:           m.Topic,
			RequiresPointer: requiresPointer,
		})
	}

	for _, join := range processor.Joins {
		var name strings.Builder
		name.WriteString("Join_")
		nameFrags := strings.Split(join.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		topicType := "protobuf"
		if join.TopicDefinition.Type != nil {
			switch *join.TopicDefinition.Type {
			case "raw":
				topicType = "raw"
			}
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]

		fullMessageTypeName := ""
		requiresPointer := false
		switch topicType {
		case "protobuf":
			requiresPointer = true

			var mPkg strings.Builder
			for _, p := range nameFrags[:len(nameFrags)-1] {
				mPkg.WriteString("/")
				mPkg.WriteString(p)
			}

			modulePackage := join.ToPackage(service)

			i, ok := imports[modulePackage]
			if !ok {
				imports[modulePackage] = importIndex
				i = importIndex

				importIndex++
			}
			fullMessageTypeName = fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message))
			args.WriteString(fmt.Sprintf(") *%s", fullMessageTypeName))
		case "raw":
			addGokaCodecs = true
			fullMessageTypeName = "[]byte"
			args.WriteString(") []byte")
		}

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Join_%s", join.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "join",
			MessageType:     fullMessageTypeName,
			MessageTypeName: join.Message,
			RequiresPointer: requiresPointer,
		}

		m.Topic = join.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fullMessageTypeName,
				Type:    topicType,
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:            "join",
			Codec:           c.Index,
			Topic:           m.Topic,
			RequiresPointer: requiresPointer,
		})
	}

	for _, output := range processor.Outputs {
		var name strings.Builder
		name.WriteString("Output_")
		nameFrags := strings.Split(output.Message, ".")
		for _, f := range nameFrags[1:] {
			name.WriteString(strcase.ToCamel(f))
		}

		topicType := "protobuf"
		if output.TopicDefinition.Type != nil {
			switch *output.TopicDefinition.Type {
			case "raw":
				topicType = "raw"
			}
		}

		var args strings.Builder
		message := nameFrags[len(nameFrags)-1]

		fullMessageTypeName := ""
		requiresPointer := false
		switch topicType {
		case "protobuf":
			requiresPointer = true
			var mPkg strings.Builder
			for _, p := range nameFrags[:len(nameFrags)-1] {
				mPkg.WriteString("/")
				mPkg.WriteString(p)
			}

			modulePackage := output.ToPackage(service)

			i, ok := imports[modulePackage]
			if !ok {
				imports[modulePackage] = importIndex
				i = importIndex

				importIndex++
			}
			fullMessageTypeName = fmt.Sprintf("m%d.%s", i, strcase.ToCamel(message))
			args.WriteString(fmt.Sprintf("key string, message *%s)", fullMessageTypeName))
		case "raw":
			addGokaCodecs = true
			fullMessageTypeName = "[]byte"
			args.WriteString("key string, message []byte)")
		}

		m := contextMethod{
			interfaceMethod: interfaceMethod{
				Name: fmt.Sprintf("Output_%s", output.ToSafeMessageTypeName()),
				Args: args.String(),
			},
			Type:            "output",
			MessageTypeName: output.Message,
			Topic:           output.ToTopicName(service),
			RequiresPointer: requiresPointer,
		}

		m.Topic = output.ToTopicName(service)

		options.Context.Methods = append(options.Context.Methods, m)

		c, ok := codecs[m.Topic]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   m.Topic,
				Message: fullMessageTypeName,
				Type:    topicType,
			}
			codecs[m.Topic] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:            "output",
			Codec:           c.Index,
			Topic:           m.Topic,
			RequiresPointer: requiresPointer,
		})
	}

	if processor.Persistence != nil {
		nameFrags := strings.Split(processor.Persistence.Message, ".")

		topicType := "protobuf"
		if processor.Persistence.TopicDefinition.Type != nil {
			switch *processor.Persistence.TopicDefinition.Type {
			case "raw":
				topicType = "raw"
			}
		}

		fullMessageTypeName := ""
		args := ""
		argsReturn := ""
		switch topicType {
		case "protobuf":

			var mPkg strings.Builder
			for _, p := range nameFrags[:len(nameFrags)-1] {
				mPkg.WriteString("/")
				mPkg.WriteString(p)
			}

			modulePackage := processor.Persistence.ToPackage(service)

			i, ok := imports[modulePackage]
			if !ok {
				imports[modulePackage] = importIndex
				i = importIndex

				importIndex++
			}
			fullMessageTypeName = fmt.Sprintf("m%d.%s", i, nameFrags[len(nameFrags)-1])
			args = fmt.Sprintf("state *%s)", fullMessageTypeName)
			argsReturn = fmt.Sprintf(") *%s", fullMessageTypeName)

		case "raw":
			fullMessageTypeName = "[]byte"
			args = fmt.Sprintf("state %s)", fullMessageTypeName)
			argsReturn = ") []byte"
		}

		options.Context.Methods = append(options.Context.Methods, contextMethod{
			interfaceMethod: interfaceMethod{
				Name: "SaveState",
				Args: args,
			},
			Type:            "save",
			MessageTypeName: processor.Persistence.Message,
			Topic:           options.Group + "-table",
		})

		options.Context.Methods = append(options.Context.Methods, contextMethod{
			interfaceMethod: interfaceMethod{
				Name: "State",
				Args: argsReturn,
			},
			Type:            "state",
			MessageType:     fullMessageTypeName,
			MessageTypeName: processor.Persistence.Message,
			Topic:           options.Group + "-table",
		})

		c, ok := codecs[options.Group+"-table"]
		if !ok {
			c = codec{
				Index:   codecIndex,
				Topic:   options.Group + "-table",
				Message: fullMessageTypeName,
				Type:    topicType,
			}
			codecs[options.Group+"-table"] = c
			codecIndex++
		}

		options.Edges = append(options.Edges, edge{
			Type:  "state",
			Codec: c.Index,
		})
	}

	for _, c := range codecs {
		options.Codecs = append(options.Codecs, c)
	}

	sort.SliceStable(options.Codecs, func(i, j int) bool {
		return options.Codecs[i].Index < options.Codecs[j].Index
	})

	for k, v := range imports {
		imp := fmt.Sprintf("m%d \"%s\"", v, k)
		options.Imports = append(options.Imports, imp)
	}

	if addGokaCodecs {
		options.Imports = append(options.Imports, "gokaCodecs \"github.com/lovoo/goka/codec\"")
	}

	sort.Strings(options.Imports)

	options.Processor = processor

	return &options, nil
}
