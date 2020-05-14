package resolvers_test

import (
	"testing"

	"gotest.tools/assert"

	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
)

func Test_Resolver(t *testing.T) {
	resolver := resolvers.NewResolver(nil, nil)

	assert.Assert(t, resolver.Query() != nil)
	assert.Assert(t, resolver.Service() != nil)
	assert.Assert(t, resolver.Component() != nil)
	assert.Assert(t, resolver.Pod() != nil)
	assert.Assert(t, resolver.Processor() != nil)
	assert.Assert(t, resolver.ProcessorInput() != nil)
	assert.Assert(t, resolver.ProcessorJoin() != nil)
	assert.Assert(t, resolver.ProcessorLookup() != nil)
	assert.Assert(t, resolver.ProcessorOutput() != nil)
	assert.Assert(t, resolver.Sink() != nil)
	assert.Assert(t, resolver.Source() != nil)
	assert.Assert(t, resolver.Topic() != nil)
	assert.Assert(t, resolver.View() != nil)
	assert.Assert(t, resolver.ViewSink() != nil)
	assert.Assert(t, resolver.ViewSource() != nil)
}
