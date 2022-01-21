package templates

import _ "embed" // required to use embeds

//go:embed header.go.tpl
var header string

//go:embed processor.go.tpl
var processor string

// Processor template
var Processor = header + processor

//go:embed service.go.tpl
var service string

// Service template
var Service = header + service

//go:embed sink.go.tpl
var sink string

// Sink template
var Sink = header + sink

//go:embed source.go.tpl
var source string

// Source template
var Source = header + source

//go:embed topic.go.tpl
var topic string

// Topic template
var Topic = header + topic

//go:embed viewSink.go.tpl
var viewSink string

// ViewSink template
var ViewSink = header + viewSink

//go:embed viewSource.go.tpl
var viewSource string

// ViewSource template
var ViewSource = header + viewSource

//go:embed view.go.tpl
var view string

// View template
var View = header + view

//go:embed discover.go.tpl
var discover string

// Discover template
var Discover = header + discover
