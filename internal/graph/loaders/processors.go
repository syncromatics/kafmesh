package loaders

import (
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/syncromatics/kafmesh/internal/graph/model"
)

// Processors contains data loaders for processor relationships
type Processors struct {
	ComponentByProcessor   *ComponentLoader
	InputsByProcessor      *InputSliceLoader
	JoinsByProcessor       *JoinSliceLoader
	LookupsByProcessor     *LookupSliceLoader
	OutputsByProcessor     *OutputSliceLoader
	PodsByProcessor        *PodSliceLoader
	PersistenceByProcessor *TopicLoader
}

func configureProcessors(loaders *Loaders) {
	loader := &Processors{}
	loaders.ProcessorLoader = loader

	loader.ComponentByProcessor = &ComponentLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Component, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
				return nil, []error{errors.Wrap(err, "failed to query for processor components")}
			}
			defer rows.Close()

			processorsComponents := map[int]*model.Component{}
			var processorID int
			for rows.Next() {
				component := &model.Component{}
				err = rows.Scan(&processorID, &component.ID, &component.Name, &component.Description)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan component row")}
				}
				processorsComponents[processorID] = component
			}

			components := []*model.Component{}
			for _, c := range processors {
				s, ok := processorsComponents[c]
				if !ok {
					return nil, []error{errors.Errorf("did not find component for processor %d", c)}
				}
				components = append(components, s)
			}

			return components, nil
		},
	}

	loader.InputsByProcessor = &InputSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorInput, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				processor,
				id
			from
				processor_inputs
			where
				processor = ANY ($1)
			`, pq.Array(processors))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processor inputs")}
			}
			defer rows.Close()

			inputs := map[int][]*model.ProcessorInput{}
			var processorID int
			for rows.Next() {
				input := &model.ProcessorInput{}
				err = rows.Scan(&processorID, &input.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor input")}
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
		},
	}

	loader.JoinsByProcessor = &JoinSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorJoin, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				processor,
				id
			from
				processor_joins
			where
				processor = ANY ($1)
			`, pq.Array(processors))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processor joins")}
			}
			defer rows.Close()

			joins := map[int][]*model.ProcessorJoin{}
			var processorID int
			for rows.Next() {
				join := &model.ProcessorJoin{}
				err = rows.Scan(&processorID, &join.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor join")}
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
		},
	}

	loader.LookupsByProcessor = &LookupSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorLookup, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				processor,
				id
			from
				processor_lookups
			where
				processor = ANY ($1)
			`, pq.Array(processors))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processor lookups")}
			}
			defer rows.Close()

			lookups := map[int][]*model.ProcessorLookup{}
			var processorID int
			for rows.Next() {
				lookup := &model.ProcessorLookup{}
				err = rows.Scan(&processorID, &lookup.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor lookup")}
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
		},
	}

	loader.OutputsByProcessor = &OutputSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.ProcessorOutput, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
			select
				processor,
				id
			from
				processor_outputs
			where
				processor = ANY ($1)
			`, pq.Array(processors))
			if err != nil {
				return nil, []error{errors.Wrap(err, "failed to query for processor outputs")}
			}
			defer rows.Close()

			outputs := map[int][]*model.ProcessorOutput{}
			var processorID int
			for rows.Next() {
				output := &model.ProcessorOutput{}
				err = rows.Scan(&processorID, &output.ID)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor output")}
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
		},
	}

	loader.PodsByProcessor = &PodSliceLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([][]*model.Pod, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
				return nil, []error{errors.Wrap(err, "failed to query for processor outputs")}
			}
			defer rows.Close()

			pods := map[int][]*model.Pod{}
			var processorID int
			for rows.Next() {
				pod := &model.Pod{}
				err = rows.Scan(&processorID, &pod.ID, &pod.Name)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan processor pods")}
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
		},
	}

	loader.PersistenceByProcessor = &TopicLoader{
		wait:     100 * time.Millisecond,
		maxBatch: 100,
		fetch: func(keys []int) ([]*model.Topic, []error) {
			var processors []int
			for _, key := range keys {
				processors = append(processors, key)
			}

			rows, err := loaders.db.QueryContext(loaders.context, `
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
				return nil, []error{errors.Wrap(err, "failed to query for processor persistence")}
			}
			defer rows.Close()

			topics := map[int]*model.Topic{}
			var processorID int
			for rows.Next() {
				topic := &model.Topic{}
				err = rows.Scan(&processorID, &topic.ID, &topic.Name, &topic.Message)
				if err != nil {
					return nil, []error{errors.Wrap(err, "failed to scan topic row")}
				}
				topics[processorID] = topic
			}

			results := []*model.Topic{}
			for _, c := range processors {
				s, _ := topics[c]
				results = append(results, s)
			}

			return results, nil
		},
	}
}
