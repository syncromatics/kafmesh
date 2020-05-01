package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ProcessorInputRepository = &ProcessorInput{}

// ProcessorInput is the repository for processor inputs
type ProcessorInput struct {
	db *sql.DB
}

// ProcessorByInputs returns processors for inputs
func (r *ProcessorInput) ProcessorByInputs(ctx context.Context, inputs []int) ([]*model.Processor, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_inputs.id,
		processors.id,
		processors.name,
		processors.description
	from
		processors
	inner join
		processor_inputs on processor_inputs.processor=processors.id
	where
		processor_inputs.id = ANY ($1)
	`, pq.Array(inputs))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for input processors")
	}
	defer rows.Close()

	processors := map[int]*model.Processor{}
	var inputID int
	for rows.Next() {
		processor := &model.Processor{}
		err = rows.Scan(&inputID, &processor.ID, &processor.Name, &processor.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor row")
		}
		processors[inputID] = processor
	}

	results := []*model.Processor{}
	for _, c := range inputs {
		s, ok := processors[c]
		if !ok {
			return nil, errors.Errorf("did not find processor for processor input %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// TopicByInputs returns topics for inputs
func (r *ProcessorInput) TopicByInputs(ctx context.Context, inputs []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_inputs.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		processor_inputs on processor_inputs.topic=topics.id
	where
		processor_inputs.id = ANY ($1)
	`, pq.Array(inputs))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor input topic")
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
	for _, c := range inputs {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
