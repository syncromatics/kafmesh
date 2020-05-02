package loaders_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"

	"github.com/golang/mock/gomock"
)

func Test_Loaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repositories := NewMockRepositories(ctrl)
	repositories.EXPECT().
		Component().
		Times(1)
	repositories.EXPECT().
		Service().
		Times(1)
	repositories.EXPECT().
		Processor().
		Times(1)
	repositories.EXPECT().
		ProcessorInput().
		Times(1)
	repositories.EXPECT().
		ProcessorJoin().
		Times(1)
	repositories.EXPECT().
		ProcessorLookup().
		Times(1)
	repositories.EXPECT().
		ProcessorOutput().
		Times(1)
	repositories.EXPECT().
		Sink().
		Times(1)
	repositories.EXPECT().
		Source().
		Times(1)
	repositories.EXPECT().
		ViewSink().
		Times(1)
	repositories.EXPECT().
		ViewSource().
		Times(1)
	repositories.EXPECT().
		Pod().
		Times(1)
	repositories.EXPECT().
		Topic().
		Times(1)
	repositories.EXPECT().
		Query().
		Times(1)
	repositories.EXPECT().
		View().
		Times(1)

	handler := loaders.NewMiddleware(repositories, 10*time.Millisecond)

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		factory := loaders.LoaderFactory{}

		assert.Assert(t, factory.ComponentLoader(r.Context()) != nil)
		assert.Assert(t, factory.PodLoader(r.Context()) != nil)
		assert.Assert(t, factory.ProcessorInputLoader(r.Context()) != nil)
		assert.Assert(t, factory.ProcessorJoinLoader(r.Context()) != nil)
		assert.Assert(t, factory.ProcessorLookupLoader(r.Context()) != nil)
		assert.Assert(t, factory.ProcessorOutputLoader(r.Context()) != nil)
		assert.Assert(t, factory.ProcessorLoader(r.Context()) != nil)
		assert.Assert(t, factory.QueryLoader(r.Context()) != nil)
		assert.Assert(t, factory.ServiceLoader(r.Context()) != nil)
		assert.Assert(t, factory.SinkLoader(r.Context()) != nil)
		assert.Assert(t, factory.SourceLoader(r.Context()) != nil)
		assert.Assert(t, factory.TopicLoader(r.Context()) != nil)
		assert.Assert(t, factory.ViewLoader(r.Context()) != nil)
		assert.Assert(t, factory.ViewSinkLoader(r.Context()) != nil)
		assert.Assert(t, factory.ViewSourceLoader(r.Context()) != nil)
	})

	rr := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	handler(next).ServeHTTP(rr, req)
}
