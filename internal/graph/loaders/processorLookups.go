package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// ProcessorLookups contains data loaders for processor lookup relationships
type ProcessorLookups struct {
	ProcessorByLookup *ProcessorLoader
	TopicByLookup     *TopicLoader
}

func configureProcessorLookups(loaders *Loaders) {
	loader := &ProcessorLookups{}
	loaders.ProcessorLookupLoader = loader

	loader.ProcessorByLookup = &ProcessorLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Processor, []error) {
			var ids []int
			for _, key := range keys {
				ids = append(ids, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
			`, pq.Array(ids))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for lookup processors")}
			}
			defer rows.Close()

			processors := map[int]*model.Processor{}
			var id int
			for rows.Next() {
				processor := &model.Processor{}
				err = rows.Scan(&id, &processor.ID, &processor.Name, &processor.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor row")}
				}
				processors[id] = processor
			}

			results := []*model.Processor{}
			for _, c := range ids {
				s, ok := processors[c]
				if !ok {
					return nil, []error{errors.Errorf("did not find processor for processor input %d", c)}
				}
				results = append(results, s)
			}

			return results, nil
		},
	}

	loader.TopicByLookup = &TopicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			var ids []int
			for _, key := range keys {
				ids = append(ids, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
			`, pq.Array(ids))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processor input topic")}
			}
			defer rows.Close()

			topics := map[int]*model.Topic{}
			var id int
			for rows.Next() {
				topic := &model.Topic{}
				err = rows.Scan(&id, &topic.ID, &topic.Name, &topic.Message)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan topic row")}
				}
				topics[id] = topic
			}

			results := []*model.Topic{}
			for _, c := range ids {
				s, _ := topics[c]
				results = append(results, s)
			}

			return results, nil
		},
	}
}
