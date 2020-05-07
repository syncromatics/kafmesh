package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ProcessorJoinRepository = &ProcessorJoin{}

// ProcessorJoin is the repository for processor joins
type ProcessorJoin struct {
	db *sql.DB
}

// ProcessorByJoins returns processors for joins
func (r *ProcessorJoin) ProcessorByJoins(ctx context.Context, joins []int) ([]*model.Processor, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_joins.id,
		processors.id,
		processors.name,
		processors.description
	from
		processors
	inner join
		processor_joins on processor_joins.processor=processors.id
	where
		processor_joins.id = ANY ($1)
	`, pq.Array(joins))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for input processors")
	}
	defer rows.Close()

	processors := map[int]*model.Processor{}
	var id int
	for rows.Next() {
		processor := &model.Processor{}
		err = rows.Scan(&id, &processor.ID, &processor.Name, &processor.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor row")
		}
		processors[id] = processor
	}

	results := []*model.Processor{}
	for _, c := range joins {
		s, ok := processors[c]
		if !ok {
			return nil, errors.Errorf("did not find processor for processor input %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// TopicByJoins returns topics for joins
func (r *ProcessorJoin) TopicByJoins(ctx context.Context, joins []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_joins.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		processor_joins on processor_joins.topic=topics.id
	where
		processor_joins.id = ANY ($1)
	`, pq.Array(joins))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor input topic")
	}
	defer rows.Close()

	topics := map[int]*model.Topic{}
	var id int
	for rows.Next() {
		topic := &model.Topic{}
		err = rows.Scan(&id, &topic.ID, &topic.Name, &topic.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan topic row")
		}
		topics[id] = topic
	}

	results := []*model.Topic{}
	for _, c := range joins {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
