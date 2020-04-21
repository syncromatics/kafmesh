package storage

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

// Deleter removes a pod and its components from storage
type Deleter struct {
	db *sql.DB
}

// NewDeleter creates a new delete
func NewDeleter(db *sql.DB) *Deleter {
	return &Deleter{db}
}

// Delete a pod from storage
func (d *Deleter) Delete(ctx context.Context, pod Pod) error {
	txn, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to being transaction")
	}
	defer txn.Rollback()

	row := txn.QueryRowContext(ctx, `
SELECT
	id
FROM
	pods
WHERE
	name=$1
	`, pod.Name)

	var podID int64
	err = row.Scan(&podID)
	if err != nil {
		return errors.Wrap(err, "failed to get pod")
	}

	deleteQuerys := []string{
		"delete from pod_processors where pod=$1;",
		"delete from pod_sources where pod=$1;",
		"delete from pod_views where pod=$1;",
		"delete from pod_sinks where pod=$1;",
		"delete from pod_view_sinks where pod=$1;",
		"delete from pod_view_sources where pod=$1;",
		"delete from pods where id=$1;",
	}

	for _, query := range deleteQuerys {
		_, err := txn.ExecContext(ctx, query, podID)
		if err != nil {
			return errors.Wrap(err, "failed to delete pod")
		}
	}

	cleanupQuerys := []string{
		`
delete from
	processor_inputs
where not exists
	(select 1 from pod_processors where pod_processors.processor=processor_inputs.processor);
		`,
		`
delete from
	processor_joins
where not exists
	(select 1 from pod_processors where pod_processors.processor=processor_joins.processor);
		`,
		`
delete from
	processor_lookups
where not exists
	(select 1 from pod_processors where pod_processors.processor=processor_lookups.processor);
		`,
		`
delete from
	processor_outputs
where not exists
	(select 1 from pod_processors where pod_processors.processor=processor_outputs.processor);
		`,
		`
delete from
	processors
where not exists
	(select 1 from pod_processors where pod_processors.processor=processors.id);
		`,
		`
delete from
	views
where not exists
	(select 1 from pod_views where pod_views.view=views.id);
		`,
		`
delete from
	sources
where not exists
	(select 1 from pod_sources where pod_sources.source=sources.id);
		`,
		`
delete from
	view_sinks
where not exists
	(select 1 from pod_view_sinks where pod_view_sinks.view_sink=view_sinks.id);
		`,
		`
delete from
	view_sources
where not exists
	(select 1 from pod_view_sources where pod_view_sources.view_source=view_sources.id);
		`,
		`
delete from
	topics
where not exists
	(
		select 1 from sources where sources.topic=topics.id union
		select 1 from views where views.topic=topics.id union
		select 1 from view_sources where view_sources.topic=topics.id union
		select 1 from view_sinks where view_sinks.topic=topics.id union
		select 1 from processors where processors.persistence=topics.id union
		select 1 from processor_inputs where processor_inputs.topic=topics.id union
		select 1 from processor_joins where processor_joins.topic=topics.id union
		select 1 from processor_lookups where processor_lookups.topic=topics.id union
		select 1 from processor_outputs where processor_outputs.topic=topics.id union
		select 1 from sinks where sinks.topic=topics.id
	);
		`,
		`
delete from
	components
where not exists
	(
		select 1 from processors where processors.component=components.id union
		select 1 from views where views.component=components.id union
		select 1 from sources where sources.component=components.id union
		select 1 from sinks where sinks.component=components.id union
		select 1 from view_sinks where view_sinks.component=components.id union
		select 1 from view_sources where view_sources.component=components.id
	)
		`,
		`
delete from
	services
where not exists
	(select 1 from components where components.service=services.id)
	`,
	}
	for _, query := range cleanupQuerys {
		_, err := txn.ExecContext(ctx, query)
		if err != nil {
			return errors.Wrap(err, "failed to exec cleanup")
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}
