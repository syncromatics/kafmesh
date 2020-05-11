package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ProcessorRepository = &Processor{}

// Processor is the repository for processors
type Processor struct {
	db *sql.DB
}

// ComponentByProcessors returns the components for processors
func (r *Processor) ComponentByProcessors(ctx context.Context, processors []int) ([]*model.Component, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processors.id,
		components.id,
		components.name,
		components.description
	from
		components
	inner join
		processors on processors.component=components.id
	where
		processors.id = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor components")
	}
	defer rows.Close()

	processorsComponents := map[int]*model.Component{}
	var processorID int
	for rows.Next() {
		component := &model.Component{}
		err = rows.Scan(&processorID, &component.ID, &component.Name, &component.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan component row")
		}
		processorsComponents[processorID] = component
	}

	components := []*model.Component{}
	for _, c := range processors {
		s, ok := processorsComponents[c]
		if !ok {
			return nil, errors.Errorf("did not find component for processor %d", c)
		}
		components = append(components, s)
	}

	return components, nil
}

// InputsByProcessors returns the inputs for processors
func (r *Processor) InputsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorInput, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor,
		id
	from
		processor_inputs
	where
		processor = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor inputs")
	}
	defer rows.Close()

	inputs := map[int][]*model.ProcessorInput{}
	var processorID int
	for rows.Next() {
		input := &model.ProcessorInput{}
		err = rows.Scan(&processorID, &input.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor input")
		}
		_, ok := inputs[processorID]
		if !ok {
			inputs[processorID] = []*model.ProcessorInput{}
		}

		inputs[processorID] = append(inputs[processorID], input)
	}

	results := [][]*model.ProcessorInput{}
	for _, s := range processors {
		_, ok := inputs[s]
		if !ok {
			results = append(results, []*model.ProcessorInput{})
		} else {
			results = append(results, inputs[s])
		}
	}
	return results, nil
}

// JoinsByProcessors returns the joins for processors
func (r *Processor) JoinsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorJoin, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor,
		id
	from
		processor_joins
	where
		processor = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor joins")
	}
	defer rows.Close()

	joins := map[int][]*model.ProcessorJoin{}
	var processorID int
	for rows.Next() {
		join := &model.ProcessorJoin{}
		err = rows.Scan(&processorID, &join.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor join")
		}
		_, ok := joins[processorID]
		if !ok {
			joins[processorID] = []*model.ProcessorJoin{}
		}

		joins[processorID] = append(joins[processorID], join)
	}

	results := [][]*model.ProcessorJoin{}
	for _, s := range processors {
		_, ok := joins[s]
		if !ok {
			results = append(results, []*model.ProcessorJoin{})
		} else {
			results = append(results, joins[s])
		}
	}
	return results, nil
}

// LookupsByProcessors returns the lookups for processors
func (r *Processor) LookupsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorLookup, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor,
		id
	from
		processor_lookups
	where
		processor = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor lookups")
	}
	defer rows.Close()

	lookups := map[int][]*model.ProcessorLookup{}
	var processorID int
	for rows.Next() {
		lookup := &model.ProcessorLookup{}
		err = rows.Scan(&processorID, &lookup.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor lookup")
		}
		_, ok := lookups[processorID]
		if !ok {
			lookups[processorID] = []*model.ProcessorLookup{}
		}

		lookups[processorID] = append(lookups[processorID], lookup)
	}

	results := [][]*model.ProcessorLookup{}
	for _, s := range processors {
		_, ok := lookups[s]
		if !ok {
			results = append(results, []*model.ProcessorLookup{})
		} else {
			results = append(results, lookups[s])
		}
	}
	return results, nil
}

// OutputsByProcessors returns the outputs for processors
func (r *Processor) OutputsByProcessors(ctx context.Context, processors []int) ([][]*model.ProcessorOutput, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor,
		id
	from
		processor_outputs
	where
		processor = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor outputs")
	}
	defer rows.Close()

	outputs := map[int][]*model.ProcessorOutput{}
	var processorID int
	for rows.Next() {
		output := &model.ProcessorOutput{}
		err = rows.Scan(&processorID, &output.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor output")
		}
		_, ok := outputs[processorID]
		if !ok {
			outputs[processorID] = []*model.ProcessorOutput{}
		}

		outputs[processorID] = append(outputs[processorID], output)
	}

	results := [][]*model.ProcessorOutput{}
	for _, s := range processors {
		_, ok := outputs[s]
		if !ok {
			results = append(results, []*model.ProcessorOutput{})
		} else {
			results = append(results, outputs[s])
		}
	}
	return results, nil
}

// PodsByProcessors returns the pods for processors
func (r *Processor) PodsByProcessors(ctx context.Context, processors []int) ([][]*model.Pod, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		pod_processors.processor,
		pods.id,
		pods.name
	from
		pods
	inner join
		pod_processors ON pod_processors.pod=pods.id
	where
		pod_processors.processor = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor outputs")
	}
	defer rows.Close()

	pods := map[int][]*model.Pod{}
	var processorID int
	for rows.Next() {
		pod := &model.Pod{}
		err = rows.Scan(&processorID, &pod.ID, &pod.Name)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor pods")
		}
		_, ok := pods[processorID]
		if !ok {
			pods[processorID] = []*model.Pod{}
		}

		pods[processorID] = append(pods[processorID], pod)
	}

	results := [][]*model.Pod{}
	for _, s := range processors {
		_, ok := pods[s]
		if !ok {
			results = append(results, []*model.Pod{})
		} else {
			results = append(results, pods[s])
		}
	}
	return results, nil
}

// PersistenceByProcessors returns the persistence topics for processors
func (r *Processor) PersistenceByProcessors(ctx context.Context, processors []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processors.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		processors on processors.persistence=topics.id
	where
		processors.id = ANY ($1)
	`, pq.Array(processors))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor persistence")
	}
	defer rows.Close()

	topics := map[int]*model.Topic{}
	var processorID int
	for rows.Next() {
		topic := &model.Topic{}
		err = rows.Scan(&processorID, &topic.ID, &topic.Name, &topic.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan topic row")
		}
		topics[processorID] = topic
	}

	results := []*model.Topic{}
	for _, c := range processors {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}

// ByID returns a processor with the id given
func (r *Processor) ByID(ctx context.Context, id int) (*model.Processor, error) {
	row := r.db.QueryRowContext(ctx, `
select
	id,
	name,
	description
from
	processors
where
	id=$1`, id)

	processor := &model.Processor{}
	err := row.Scan(&processor.ID, &processor.Name, &processor.Description)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan for processor")
	}

	return processor, nil
}
