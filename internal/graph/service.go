package graph

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql/handler/extension"

	"github.com/syncromatics/kafmesh/internal/graph/generated"
	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/resolvers"
	"github.com/syncromatics/kafmesh/internal/graph/subscription"
	"github.com/syncromatics/kafmesh/internal/storage/repositories"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/rs/cors"
	"github.com/syncromatics/go-kit/log"
)

// Service hosts the graphql api
type Service struct {
	port      int
	db        *sql.DB
	podLister subscription.PodLister
}

// NewService creates a new graphql service
func NewService(port int, db *sql.DB, podLister subscription.PodLister) *Service {
	return &Service{port, db, podLister}
}

// Run the graphql api
func (s *Service) Run(ctx context.Context) func() error {
	repositories := repositories.All(s.db)

	router := chi.NewRouter()
	router.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	router.Use(loaders.NewMiddleware(repositories, 30*time.Millisecond))

	srv := &http.Server{Addr: fmt.Sprintf(":%d", s.port), Handler: router}
	srv.SetKeepAlivesEnabled(true)

	subscriber := subscription.NewSubscribers(s.podLister, repositories.Processor())
	resolver := resolvers.NewResolver(&loaders.LoaderFactory{}, subscriber)

	server := handler.New(generated.NewExecutableSchema(generated.Config{
		Resolvers: resolver,
	}))

	server.Use(extension.Introspection{})
	server.AddTransport(transport.POST{})
	server.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	})

	server.SetRecoverFunc(func(ctx context.Context, err interface{}) error {
		log.Error("failed graphql request", "path", graphql.GetResolverContext(ctx).Path(), "error", err)
		return errors.Errorf("internal server error")
	})

	router.Handle("/", playground.Handler("kafmesh", "/query"))
	router.Handle("/query", server)

	cancel := make(chan error)

	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			cancel <- errors.Wrap(err, "failed to serve http")
		}
	}()

	return func() error {
		select {
		case <-ctx.Done():
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			srv.Shutdown(ctx)
			return nil
		case msg := <-cancel:
			return msg
		}
	}
}
