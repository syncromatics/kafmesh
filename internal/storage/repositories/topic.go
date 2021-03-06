package repositories

import (
	"context"
	"database/sql"

	"github.com/syncromatics/kafmesh/internal/graph/loaders"
	"github.com/syncromatics/kafmesh/internal/graph/model"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ loaders.TopicRepository = &Topic{}

// Topic is the repository for topics
type Topic struct {
	db *sql.DB
}

// ProcessorInputsByTopics returns the processor inputs for topics
func (r *Topic) ProcessorInputsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorInput, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		processor_inputs.id
	from
		processor_inputs
	inner join
		topics on topics.id=processor_inputs.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor inputs")
	}
	defer rows.Close()

	inputs := map[int][]*model.ProcessorInput{}
	var id int
	for rows.Next() {
		input := &model.ProcessorInput{}
		err = rows.Scan(&id, &input.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor input")
		}
		_, ok := inputs[id]
		if !ok {
			inputs[id] = []*model.ProcessorInput{}
		}

		inputs[id] = append(inputs[id], input)
	}

	results := [][]*model.ProcessorInput{}
	for _, s := range topics {
		_, ok := inputs[s]
		if !ok {
			results = append(results, []*model.ProcessorInput{})
		} else {
			results = append(results, inputs[s])
		}
	}
	return results, nil
}

// ProcessorJoinsByTopics returns the processor joins for topics
func (r *Topic) ProcessorJoinsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorJoin, error) {
	rows, err := r.db.QueryContext(ctx, `
			select
				topics.id,
				processor_joins.id
			from
				processor_joins
			inner join
				topics on topics.id=processor_joins.topic
			where
				topics.id = ANY ($1)
			`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor joins")
	}
	defer rows.Close()

	joins := map[int][]*model.ProcessorJoin{}
	var id int
	for rows.Next() {
		join := &model.ProcessorJoin{}
		err = rows.Scan(&id, &join.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor join")
		}
		_, ok := joins[id]
		if !ok {
			joins[id] = []*model.ProcessorJoin{}
		}

		joins[id] = append(joins[id], join)
	}

	results := [][]*model.ProcessorJoin{}
	for _, s := range topics {
		_, ok := joins[s]
		if !ok {
			results = append(results, []*model.ProcessorJoin{})
		} else {
			results = append(results, joins[s])
		}
	}
	return results, nil
}

// ProcessorLookupsByTopics returns the processor lookups for topics
func (r *Topic) ProcessorLookupsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorLookup, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		processor_lookups.id
	from
		processor_lookups
	inner join
		topics on topics.id=processor_lookups.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor lookups")
	}
	defer rows.Close()

	lookups := map[int][]*model.ProcessorLookup{}
	var id int
	for rows.Next() {
		lookup := &model.ProcessorLookup{}
		err = rows.Scan(&id, &lookup.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor lookup")
		}
		_, ok := lookups[id]
		if !ok {
			lookups[id] = []*model.ProcessorLookup{}
		}

		lookups[id] = append(lookups[id], lookup)
	}

	results := [][]*model.ProcessorLookup{}
	for _, s := range topics {
		_, ok := lookups[s]
		if !ok {
			results = append(results, []*model.ProcessorLookup{})
		} else {
			results = append(results, lookups[s])
		}
	}
	return results, nil
}

// ProcessorOutputsByTopics returns the processor outputs for topics
func (r *Topic) ProcessorOutputsByTopics(ctx context.Context, topics []int) ([][]*model.ProcessorOutput, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		processor_outputs.id
	from
		processor_outputs
	inner join
		topics on topics.id=processor_outputs.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processor outputs")
	}
	defer rows.Close()

	outputs := map[int][]*model.ProcessorOutput{}
	var id int
	for rows.Next() {
		output := &model.ProcessorOutput{}
		err = rows.Scan(&id, &output.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor output")
		}
		_, ok := outputs[id]
		if !ok {
			outputs[id] = []*model.ProcessorOutput{}
		}

		outputs[id] = append(outputs[id], output)
	}

	results := [][]*model.ProcessorOutput{}
	for _, s := range topics {
		_, ok := outputs[s]
		if !ok {
			results = append(results, []*model.ProcessorOutput{})
		} else {
			results = append(results, outputs[s])
		}
	}
	return results, nil
}

// ProcessorPersistencesByTopics returns the process persistences for topics
func (r *Topic) ProcessorPersistencesByTopics(ctx context.Context, topics []int) ([][]*model.Processor, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		processors.id,
		processors.name,
		processors.description,
		processors.group_name
	from
		processors
	inner join
		topics on topics.id=processors.persistence
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for processors")
	}
	defer rows.Close()

	processors := map[int][]*model.Processor{}
	var id int
	for rows.Next() {
		processor := &model.Processor{}
		err = rows.Scan(&id, &processor.ID, &processor.Name, &processor.Description, &processor.GroupName)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan processor")
		}
		_, ok := processors[id]
		if !ok {
			processors[id] = []*model.Processor{}
		}

		processors[id] = append(processors[id], processor)
	}

	results := [][]*model.Processor{}
	for _, s := range topics {
		_, ok := processors[s]
		if !ok {
			results = append(results, []*model.Processor{})
		} else {
			results = append(results, processors[s])
		}
	}
	return results, nil
}

// SinksByTopics returns the sinks for topics
func (r *Topic) SinksByTopics(ctx context.Context, topics []int) ([][]*model.Sink, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		sinks.id,
		sinks.name,
		sinks.description
	from
		sinks
	inner join
		topics on topics.id=sinks.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for sinks")
	}
	defer rows.Close()

	sinks := map[int][]*model.Sink{}
	var id int
	for rows.Next() {
		sink := &model.Sink{}
		err = rows.Scan(&id, &sink.ID, &sink.Name, &sink.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan sink")
		}
		_, ok := sinks[id]
		if !ok {
			sinks[id] = []*model.Sink{}
		}

		sinks[id] = append(sinks[id], sink)
	}

	results := [][]*model.Sink{}
	for _, s := range topics {
		_, ok := sinks[s]
		if !ok {
			results = append(results, []*model.Sink{})
		} else {
			results = append(results, sinks[s])
		}
	}
	return results, nil
}

// SourcesByTopics returns the sources for topics
func (r *Topic) SourcesByTopics(ctx context.Context, topics []int) ([][]*model.Source, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		sources.id
	from
		sources
	inner join
		topics on topics.id=sources.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for sources")
	}
	defer rows.Close()

	sources := map[int][]*model.Source{}
	var id int
	for rows.Next() {
		source := &model.Source{}
		err = rows.Scan(&id, &source.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan source")
		}
		_, ok := sources[id]
		if !ok {
			sources[id] = []*model.Source{}
		}

		sources[id] = append(sources[id], source)
	}

	results := [][]*model.Source{}
	for _, s := range topics {
		_, ok := sources[s]
		if !ok {
			results = append(results, []*model.Source{})
		} else {
			results = append(results, sources[s])
		}
	}
	return results, nil
}

// ViewSinksByTopics returns the view sinks for topics
func (r *Topic) ViewSinksByTopics(ctx context.Context, topics []int) ([][]*model.ViewSink, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		view_sinks.id,
		view_sinks.name,
		view_sinks.description
	from
		view_sinks
	inner join
		topics on topics.id=view_sinks.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for view sinks")
	}
	defer rows.Close()

	viewSinks := map[int][]*model.ViewSink{}
	var id int
	for rows.Next() {
		viewSink := &model.ViewSink{}
		err = rows.Scan(&id, &viewSink.ID, &viewSink.Name, &viewSink.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan view sink")
		}
		_, ok := viewSinks[id]
		if !ok {
			viewSinks[id] = []*model.ViewSink{}
		}

		viewSinks[id] = append(viewSinks[id], viewSink)
	}

	results := [][]*model.ViewSink{}
	for _, s := range topics {
		_, ok := viewSinks[s]
		if !ok {
			results = append(results, []*model.ViewSink{})
		} else {
			results = append(results, viewSinks[s])
		}
	}
	return results, nil
}

// ViewSourcesByTopics returns the view sources for topics
func (r *Topic) ViewSourcesByTopics(ctx context.Context, topics []int) ([][]*model.ViewSource, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		view_sources.id,
		view_sources.name,
		view_sources.description
	from
		view_sources
	inner join
		topics on topics.id=view_sources.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for view sources")
	}
	defer rows.Close()

	viewSources := map[int][]*model.ViewSource{}
	var id int
	for rows.Next() {
		viewSource := &model.ViewSource{}
		err = rows.Scan(&id, &viewSource.ID, &viewSource.Name, &viewSource.Description)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan view source")
		}
		_, ok := viewSources[id]
		if !ok {
			viewSources[id] = []*model.ViewSource{}
		}

		viewSources[id] = append(viewSources[id], viewSource)
	}

	results := [][]*model.ViewSource{}
	for _, s := range topics {
		_, ok := viewSources[s]
		if !ok {
			results = append(results, []*model.ViewSource{})
		} else {
			results = append(results, viewSources[s])
		}
	}
	return results, nil
}

// ViewsByTopics returns the views for topics
func (r *Topic) ViewsByTopics(ctx context.Context, topics []int) ([][]*model.View, error) {
	rows, err := r.db.QueryContext(ctx, `
	select
		topics.id,
		views.id
	from
		views
	inner join
		topics on topics.id=views.topic
	where
		topics.id = ANY ($1)
	`, pq.Array(topics))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query for views")
	}
	defer rows.Close()

	views := map[int][]*model.View{}
	var id int
	for rows.Next() {
		view := &model.View{}
		err = rows.Scan(&id, &view.ID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan view")
		}
		_, ok := views[id]
		if !ok {
			views[id] = []*model.View{}
		}

		views[id] = append(views[id], view)
	}

	results := [][]*model.View{}
	for _, s := range topics {
		_, ok := views[s]
		if !ok {
			results = append(results, []*model.View{})
		} else {
			results = append(results, views[s])
		}
	}
	return results, nil
}
