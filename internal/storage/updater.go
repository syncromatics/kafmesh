package storage

import (
	"context"
	"database/sql"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// Pod is a kubernetes pod
type Pod struct {
	Name string
}

// Updater updates and inserts pods in storage
type Updater struct {
	db *sql.DB
}

// NewUpdater creates a new updater
func NewUpdater(db *sql.DB) *Updater {
	return &Updater{db}
}

// Update inserts a pod and service into storage
func (u *Updater) Update(ctx context.Context, pod Pod, service *discoveryv1.Service) error {
	err := u.updateService(ctx, service, pod)
	if err != nil {
		return errors.Wrapf(err, "failed to update service '%s'", service.Name)
	}
	return nil
}

func (u *Updater) updateService(ctx context.Context, service *discoveryv1.Service, pod Pod) error {
	txn, err := u.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	defer txn.Rollback()

	r := txn.QueryRowContext(ctx, `
INSERT INTO
	services(name, description) 
	VALUES($1,$2)
ON CONFLICT(name)
	DO UPDATE SET 
		name = EXCLUDED.name,
		description = EXCLUDED.description
RETURNING id;
`, service.Name, service.Description)

	var serviceID int64
	err = r.Scan(&serviceID)
	if err != nil {
		return errors.Wrap(err, "failed to upsert service")
	}

	for _, component := range service.Components {
		err = u.updateComponent(ctx, txn, serviceID, component, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to update component '%s'", component.Name)
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func (u *Updater) updateComponent(ctx context.Context, txn *sql.Tx, serviceID int64, component *discoveryv1.Component, pod Pod) error {
	r := txn.QueryRowContext(ctx, `
	INSERT INTO
		components(service, name, description) 
		VALUES($1,$2,$3)
	ON CONFLICT(service, name)
		DO UPDATE SET 
			name = EXCLUDED.name,
			description = EXCLUDED.description
	RETURNING id;
	`, serviceID, component.Name, component.Description)

	var componentID int64
	err := r.Scan(&componentID)
	if err != nil {
		return errors.Wrap(err, "failed to upsert component")
	}

	for _, processor := range component.Processors {
		err := u.updateProcessor(ctx, txn, componentID, processor, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create processor '%s'", processor.Name)
		}
	}

	for _, source := range component.Sources {
		err := u.updateSource(ctx, txn, componentID, source, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create source '%s'", source.Topic.Topic)
		}
	}

	for _, view := range component.Views {
		err := u.updateView(ctx, txn, componentID, view, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create view '%s'", view.Topic.Topic)
		}
	}

	for _, sink := range component.Sinks {
		err := u.updateSink(ctx, txn, componentID, sink, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create sink '%s'", sink.Name)
		}
	}

	for _, viewSink := range component.ViewSinks {
		err := u.updateViewSink(ctx, txn, componentID, viewSink, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create view sink '%s'", viewSink.Name)
		}
	}

	for _, viewSource := range component.ViewSources {
		err := u.updateViewSource(ctx, txn, componentID, viewSource, pod)
		if err != nil {
			return errors.Wrapf(err, "failed to create view source '%s'", viewSource.Name)
		}
	}

	return nil
}

func (u *Updater) updateProcessor(ctx context.Context, txn *sql.Tx, componentID int64, processor *discoveryv1.Processor, pod Pod) error {
	r := txn.QueryRowContext(ctx, `
	WITH processor as (
		INSERT INTO
			processors(component, name, description, group_name) 
			VALUES($1,$2,$3,$4)
		ON CONFLICT(component, name)
			DO UPDATE SET 
				name = EXCLUDED.name,
				description = EXCLUDED.description,
				group_name = EXCLUDED.group_name
		RETURNING id
	), pod AS (
		INSERT INTO
			pods(name) 
			VALUES($5)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	)
	INSERT INTO
		pod_processors(pod, processor)
	SELECT
		pod.id,
		processor.id
	FROM
		processor
	JOIN
		pod on true
	RETURNING processor;
	`, componentID, processor.Name, processor.Description, processor.GroupName, pod.Name)

	var processorID int64
	err := r.Scan(&processorID)
	if err != nil {
		return errors.Wrap(err, "failed to upsert component")
	}

	if processor.Persistence != nil {
		_, err = txn.ExecContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	)
	update processors
		set persistence=new_topic.id
	from
		new_topic
	where 
		processors.id=$3;
	`, processor.Persistence.Topic.Topic, processor.Persistence.Topic.Message, processorID)
		if err != nil {
			return errors.Wrap(err, "failed to upsert topic persistence")
		}
	} else {
		_, err = txn.ExecContext(ctx, `
	update processors
		set persistence=null
	where 
		id=$1;
	`, processorID)
		if err != nil {
			return errors.Wrap(err, "failed to upsert topic persistence")
		}
	}

	inputs := []int64{}
	for _, input := range processor.Inputs {
		id, err := u.updateProcessorInput(ctx, txn, processorID, input)
		if err != nil {
			return errors.Wrap(err, "failed to upsert input")
		}
		inputs = append(inputs, id)
	}

	joins := []int64{}
	for _, join := range processor.Joins {
		id, err := u.updateProcessorJoin(ctx, txn, processorID, join)
		if err != nil {
			return errors.Wrap(err, "failed to upsert join")
		}
		joins = append(joins, id)
	}

	lookups := []int64{}
	for _, lookup := range processor.Lookups {
		id, err := u.updateProcessorLookup(ctx, txn, processorID, lookup)
		if err != nil {
			return errors.Wrap(err, "failed to upsert lookup")
		}
		lookups = append(lookups, id)
	}

	outputs := []int64{}
	for _, output := range processor.Outputs {
		id, err := u.updateProcessorOutput(ctx, txn, processorID, output)
		if err != nil {
			return errors.Wrap(err, "failed to upsert output")
		}
		outputs = append(outputs, id)
	}

	_, err = txn.ExecContext(ctx, `
	delete from processor_inputs where processor=$1 and not (id = ANY($2));
	`, processorID, pq.Array(inputs))
	if err != nil {
		return errors.Wrap(err, "failed to cleanup inputs")
	}

	_, err = txn.ExecContext(ctx, `
	delete from processor_joins where processor=$1 and not (id = ANY($2));
	`, processorID, pq.Array(joins))
	if err != nil {
		return errors.Wrap(err, "failed to cleanup joins")
	}

	_, err = txn.ExecContext(ctx, `
	delete from processor_lookups where processor=$1 and not (id = ANY($2));
	`, processorID, pq.Array(lookups))
	if err != nil {
		return errors.Wrap(err, "failed to cleanup lookups")
	}

	_, err = txn.ExecContext(ctx, `
	delete from processor_outputs where processor=$1 and not (id = ANY($2));
	`, processorID, pq.Array(outputs))
	if err != nil {
		return errors.Wrap(err, "failed to cleanup outputs")
	}

	return nil
}

func (u *Updater) updateProcessorInput(ctx context.Context, txn *sql.Tx, processorID int64, input *discoveryv1.Input) (int64, error) {
	r := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	)
	INSERT INTO
		processor_inputs(processor, topic)
		select $3, new_topic.id from new_topic
	ON CONFLICT(processor, topic)
		DO UPDATE SET
			topic=EXCLUDED.topic
	RETURNING id;
	`, input.Topic.Topic, input.Topic.Message, processorID)

	var inputID int64
	err := r.Scan(&inputID)
	if err != nil {
		return -1, errors.Wrap(err, "failed to upsert input")
	}
	return inputID, nil
}

func (u *Updater) updateProcessorJoin(ctx context.Context, txn *sql.Tx, processorID int64, join *discoveryv1.Join) (int64, error) {
	r := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	)
	INSERT INTO
		processor_joins(processor, topic)
		select $3, new_topic.id from new_topic
	ON CONFLICT(processor, topic)
		DO UPDATE SET
			topic=EXCLUDED.topic
	RETURNING id;
	`, join.Topic.Topic, join.Topic.Message, processorID)

	var joinID int64
	err := r.Scan(&joinID)
	if err != nil {
		return -1, errors.Wrap(err, "failed to upsert join")
	}
	return joinID, nil
}

func (u *Updater) updateProcessorLookup(ctx context.Context, txn *sql.Tx, processorID int64, lookup *discoveryv1.Lookup) (int64, error) {
	r := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	)
	INSERT INTO
		processor_lookups(processor, topic)
		select $3, new_topic.id from new_topic
	ON CONFLICT(processor, topic)
		DO UPDATE SET
			topic=EXCLUDED.topic
	RETURNING id;
	`, lookup.Topic.Topic, lookup.Topic.Message, processorID)

	var lookupID int64
	err := r.Scan(&lookupID)
	if err != nil {
		return -1, errors.Wrap(err, "failed to upsert lookup")
	}
	return lookupID, nil
}

func (u *Updater) updateProcessorOutput(ctx context.Context, txn *sql.Tx, processorID int64, output *discoveryv1.Output) (int64, error) {
	r := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	)
	INSERT INTO
		processor_outputs(processor, topic)
		select $3, new_topic.id from new_topic
	ON CONFLICT(processor, topic)
		DO UPDATE SET
			topic=EXCLUDED.topic
	RETURNING id;
	`, output.Topic.Topic, output.Topic.Message, processorID)

	var outputID int64
	err := r.Scan(&outputID)
	if err != nil {
		return -1, errors.Wrap(err, "failed to upsert output")
	}
	return outputID, nil
}

func (u *Updater) updateSource(ctx context.Context, txn *sql.Tx, componentID int64, source *discoveryv1.Source, pod Pod) error {
	row := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	), source as(
		INSERT INTO
			sources(component, topic)
			select $3, new_topic.id from new_topic
		ON CONFLICT(component, topic)
			DO UPDATE SET
				topic=EXCLUDED.topic
		RETURNING id
	), pod as(
		INSERT INTO
			pods(name) 
			VALUES($4)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	)
	INSERT INTO
		pod_sources(pod, source)
	SELECT
		pod.id,
		source.id
	FROM
		pod
	JOIN
		source on true
	RETURNING source;
	`, source.Topic.Topic, source.Topic.Message, componentID, pod.Name)

	var sourceID int64
	err := row.Scan(&sourceID)
	if err != nil {
		return errors.Wrap(err, "failed to insert source")
	}

	return err
}

func (u *Updater) updateView(ctx context.Context, txn *sql.Tx, componentID int64, view *discoveryv1.View, pod Pod) error {
	row := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	), pod as(
		INSERT INTO
			pods(name) 
			VALUES($4)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	), view AS(
		INSERT INTO
			views(component, topic)
			select $3, new_topic.id from new_topic
		ON CONFLICT(component, topic)
			DO UPDATE SET
				topic=EXCLUDED.topic
		RETURNING id
	)
	INSERT INTO
		pod_views(pod, view)
	SELECT
		pod.id,
		view.id
	FROM
		pod, view
	RETURNING view;
	`, view.Topic.Topic, view.Topic.Message, componentID, pod.Name)

	var id int64
	err := row.Scan(&id)
	if err != nil {
		return errors.Wrap(err, "failed to insert view")
	}

	return err
}

func (u *Updater) updateSink(ctx context.Context, txn *sql.Tx, componentID int64, sink *discoveryv1.Sink, pod Pod) error {
	row := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	), pod as(
		INSERT INTO
			pods(name)
			VALUES($6)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	), sink AS(
		INSERT INTO
			sinks(component, name, topic, description)
			select $3, $4, new_topic.id, $5 from new_topic
		ON CONFLICT(component, name)
			DO UPDATE SET
				description = EXCLUDED.description,
				topic = EXCLUDED.topic
		RETURNING id
	)
	INSERT INTO
		pod_sinks(pod, sink)
	SELECT
		pod.id,
		sink.id
	FROM
		pod, sink
	RETURNING sink;
	`, sink.Topic.Topic, sink.Topic.Message, componentID, sink.Name, sink.Description, pod.Name)

	var id int64
	err := row.Scan(&id)
	if err != nil {
		return errors.Wrap(err, "failed to insert sink")
	}

	return err
}

func (u *Updater) updateViewSink(ctx context.Context, txn *sql.Tx, componentID int64, sink *discoveryv1.ViewSink, pod Pod) error {
	row := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	), pod as(
		INSERT INTO
			pods(name)
			VALUES($6)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	), view_sink AS(
		INSERT INTO
			view_sinks(component, name, topic, description)
			select $3, $4, new_topic.id, $5 from new_topic
		ON CONFLICT(component, name)
			DO UPDATE SET
				description = EXCLUDED.description,
				topic = EXCLUDED.topic
		RETURNING id
	)
	INSERT INTO
		pod_view_sinks(pod, view_sink)
	SELECT
		pod.id,
		view_sink.id
	FROM
		pod, view_sink
	RETURNING view_sink;
	`, sink.Topic.Topic, sink.Topic.Message, componentID, sink.Name, sink.Description, pod.Name)

	var id int64
	err := row.Scan(&id)
	if err != nil {
		return errors.Wrap(err, "failed to insert view sink")
	}

	return err
}

func (u *Updater) updateViewSource(ctx context.Context, txn *sql.Tx, componentID int64, source *discoveryv1.ViewSource, pod Pod) error {
	row := txn.QueryRowContext(ctx, `
	WITH new_topic as(
		INSERT INTO
			topics(name, message) 
			VALUES($1,$2)
		ON CONFLICT(name)
			DO UPDATE SET
				message = EXCLUDED.message
		RETURNING id
	), pod as(
		INSERT INTO
			pods(name)
			VALUES($6)
		ON CONFLICT(name)
			DO UPDATE SET
				name=EXCLUDED.name
		RETURNING id
	), view_source AS(
		INSERT INTO
			view_sources(component, name, topic, description)
			select $3, $4, new_topic.id, $5 from new_topic
		ON CONFLICT(component, name)
			DO UPDATE SET
				description = EXCLUDED.description,
				topic = EXCLUDED.topic
		RETURNING id
	)
	INSERT INTO
		pod_view_sources(pod, view_source)
	SELECT
		pod.id,
		view_source.id
	FROM
		pod, view_source
	RETURNING view_source;
	`, source.Topic.Topic, source.Topic.Message, componentID, source.Name, source.Description, pod.Name)

	var id int64
	err := row.Scan(&id)
	if err != nil {
		return errors.Wrap(err, "failed to insert view source")
	}

	return err
}
