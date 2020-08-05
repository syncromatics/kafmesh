package repositories_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/syncromatics/kafmesh/internal/storage/repositories"

	"github.com/syncromatics/go-kit/database"
	"github.com/syncromatics/go-kit/testing/docker"

	_ "github.com/syncromatics/kafmesh/internal/storage/statik"
)

var (
	databaseSettings *database.PostgresDatabaseSettings
	db               *sql.DB
	repos            *repositories.AllRepositories
)

func TestMain(m *testing.M) {
	var err error
	db, databaseSettings, err = docker.SetupPostgresDatabase("repositories_storage")
	if err != nil {
		panic(err)
	}

	err = databaseSettings.MigrateUpWithStatik("/")
	if err != nil {
		panic(err)
	}

	err = seedData(db)
	if err != nil {
		panic(err)
	}

	repos = repositories.All(db)

	result := m.Run()

	docker.TeardownPostgresDatabase("storage")

	os.Exit(result)
}

func seedData(db *sql.DB) error {
	_, err := db.Exec(`
insert into
	topics
		(id, name, message)
	values
		(1, 'topic1', 'topic1.message'),
		(2, 'topic2', 'topic2.message'),
		(3, 'topic3', 'topic3.message'),
		(4, 'topic4', 'topic4.message');

insert into
	services
		(id, name, description)
	values
		(1, 'service1', 'service1 description'),
		(2, 'service2', 'service2 description'),
		(3, 'service3', 'service3 description'),
		(4, 'service4', 'service4 description');

insert into
	components
		(id, service, name, description)
	values
		(1, 1, 'component1', 'component1 description'),
		(2, 1, 'component2', 'component2 description'),
		(3, 2, 'component3', 'component3 description'),
		(4, 2, 'component4', 'component4 description');

insert into
	processors
		(id, component, name, description, group_name, persistence)
	values
		(1, 1, 'processor1', 'processor1 description', 'processor1.group', null),
		(2, 1, 'processor2', 'processor2 description', 'processor2.group', 1),
		(3, 2, 'processor3', 'processor3 description', 'processor3.group', 2),
		(4, 2, 'processor4', 'processor4 description', 'processor4.group', 2);

insert into
	processor_inputs
		(id, processor, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	processor_joins
		(id, processor, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	processor_lookups
		(id, processor, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	processor_outputs
		(id, processor, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	sources
		(id, component, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	views
		(id, component, topic)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2),
		(5, 3, 1);

insert into
	sinks
		(id, component, topic, name, description)
	values
		(1, 1, 1, 'sink1', 'sink1 description'),
		(2, 1, 2, 'sink2', 'sink2 description'),
		(3, 2, 1, 'sink3', 'sink3 description'),
		(4, 2, 2, 'sink4', 'sink4 description');

insert into
	view_sinks
		(id, component, topic, name, description)
	values
		(1, 1, 1, 'viewSink1', 'viewSink1 description'),
		(2, 1, 2, 'viewSink2', 'viewSink2 description'),
		(3, 2, 1, 'viewSink3', 'viewSink3 description'),
		(4, 2, 2, 'viewSink4', 'viewSink4 description');

insert into
	view_sources
		(id, component, topic, name, description)
	values
		(1, 1, 1, 'viewSource1', 'viewSource1 description'),
		(2, 1, 2, 'viewSource2', 'viewSource2 description'),
		(3, 2, 1, 'viewSource3', 'viewSource3 description'),
		(4, 2, 2, 'viewSource4', 'viewSource4 description');

insert into
	pods
		(id, name)
	values
		(1, 'pod1'),
		(2, 'pod2');

insert into
	pod_processors
		(id, pod, processor)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	pod_sources
		(id, pod, source)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	pod_views
		(id, pod, view)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	pod_sinks
		(id, pod, sink)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	pod_view_sinks
		(id, pod, view_sink)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);

insert into
	pod_view_sources
		(id, pod, view_source)
	values
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 1),
		(4, 2, 2);
	`)
	return err
}
