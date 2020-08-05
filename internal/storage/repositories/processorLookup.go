package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.ProcessorLookupRepository = &ProcessorLookup{}

// ProcessorLookup is the repository for processor lookups
type ProcessorLookup struct {
	db *sql.DB
}

// ProcessorByLookups returns processors for lookups
func (r *ProcessorLookup) ProcessorByLookups(ctx context.Context, lookups []int) ([]*model.Processor, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_lookups.id,
		processors.id,
		processors.name,
		processors.description
	from
		processors
	inner join
		processor_lookups on processor_lookups.processor=processors.id
	where
		processor_lookups.id = ANY ($1)
	`, pq.Array(lookups))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for lookup processors")
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
	for _, c := range lookups {
		s, ok := processors[c]
		if !ok {
			return nil, errors.Errorf("did not find processor for processor input %d", c)
		}
		results = append(results, s)
	}

	return results, nil
}

// TopicByLookups returns topics for lookups
func (r *ProcessorLookup) TopicByLookups(ctx context.Context, lookups []int) ([]*model.Topic, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		processor_lookups.id,
		topics.id,
		topics.name,
		topics.message
	from
		topics
	inner join
		processor_lookups on processor_lookups.topic=topics.id
	where
		processor_lookups.id = ANY ($1)
	`, pq.Array(lookups))
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
	for _, c := range lookups {
		s, _ := topics[c]
		results = append(results, s)
	}

	return results, nil
}
