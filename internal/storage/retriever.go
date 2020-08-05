package storage

import (
	"context"
	"database/sql"

	discoveryv1 "github.com/syncromatics/kafmesh/internal/protos/kafmesh/discovery/v1"

	"github.com/pkg/errors"
)

// Retriever retrieves data from storage
type Retriever struct {
	db *sql.DB
}

// NewRetriever creates a new retriever
func NewRetriever(db *sql.DB) *Retriever {
	return &Retriever{db}
}

// GetPods retrieves all the pods in storage
func (r *Retriever) GetPods(ctx context.Context) (map[string]struct{}, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT
	name
FROM
	pods
`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pods")
	}
	defer rows.Close()

	result := map[string]struct{}{}
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		result[name] = struct{}{}
	}
	return result, nil
}

// GetServiceForPod gets the kafmesh service for a pod
func (r *Retriever) GetServiceForPod(ctx context.Context, pod string) (*discoveryv1.Service, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT
	id
FROM
	pods
WHERE
	name=$1
`, pod)

	var podID int64
	err := row.Scan(&podID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pod")
	}

	service, err := r.getService(ctx, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get service")
	}

	return service, nil
}

func (r *Retriever) getService(ctx context.Context, pod string) (*discoveryv1.Service, error) {
	rows, err := r.db.QueryContext(ctx, `
	SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_processors pp on p.id = pp.pod
	JOIN
		processors processor on processor.id = pp.processor
	JOIN
		components c on c.id = processor.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1

	UNION SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_sources ps on p.id = ps.pod
	JOIN
		sources source on source.id = ps.source
	JOIN
		components c on c.id = source.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1

	UNION SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_views pv on p.id = pv.pod
	JOIN
		views view on view.id = pv.view
	JOIN
		components c on c.id = view.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1

	UNION SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_sinks pv on p.id = pv.pod
	JOIN
		sinks sink on sink.id = pv.sink
	JOIN
		components c on c.id = sink.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1

	UNION SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_view_sinks pv on p.id = pv.pod
	JOIN
		view_sinks sink on sink.id = pv.view_sink
	JOIN
		components c on c.id = sink.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1

	UNION SELECT
		s.name,
		s.description,
		c.name as component_name,
		c.description as component_description
	FROM
		pods p
	JOIN
		pod_view_sources pv on p.id = pv.pod
	JOIN
		view_sources source on source.id = pv.view_source
	JOIN
		components c on c.id = source.component
	JOIN
		services s on s.id = c.service
	WHERE
		p.name=$1
`, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to service components")
	}
	defer rows.Close()

	service := &discoveryv1.Service{}

	for rows.Next() {
		var name, description, componentName, componentDescription string
		err := rows.Scan(&name, &description, &componentName, &componentDescription)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan service row")
		}
		service.Name = name
		service.Description = description

		service.Components = append(service.Components, &discoveryv1.Component{
			Name:        componentName,
			Description: componentDescription,
		})
	}

	err = r.getProcessors(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get processors")
	}

	err = r.getSources(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sources")
	}

	err = r.getViews(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get views")
	}

	err = r.getSinks(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get sinks")
	}

	err = r.getViewSinks(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sinks")
	}

	err = r.getViewSources(ctx, service, pod)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get view sources")
	}

	return service, nil
}

func (r *Retriever) getProcessors(ctx context.Context, service *discoveryv1.Service, pod string) error {
	componentProcessors := map[string]map[string]*discoveryv1.Processor{}

	getProcessor := func(component, name, description, groupName string) *discoveryv1.Processor {
		c, ok := componentProcessors[component]
		if !ok {
			c = map[string]*discoveryv1.Processor{}
			componentProcessors[component] = c
		}

		p, ok := c[name]
		if !ok {
			p = &discoveryv1.Processor{
				Name:        name,
				Description: description,
				GroupName:   groupName,
			}
			c[name] = p
		}

		return p
	}

	//inputs
	rows, err := r.db.QueryContext(ctx, `
	SELECT
		processors.name,
		processors.description,
		processors.group_name,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_processors on pods.id = pod_processors.pod
	JOIN
		processors on processors.id = pod_processors.processor
	JOIN
		components on components.id = processors.component
	JOIN
		processor_inputs on processor_inputs.processor = processors.id
	JOIN
		topics on topics.id = processor_inputs.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, groupName, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &groupName, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan topic input")
		}

		p := getProcessor(componentName, name, description, groupName)

		p.Inputs = append(p.Inputs, &discoveryv1.Input{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	// joins
	rows, err = r.db.QueryContext(ctx, `
	SELECT
		processors.name,
		processors.description,
		processors.group_name,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_processors on pods.id = pod_processors.pod
	JOIN
		processors on processors.id = pod_processors.processor
	JOIN
		components on components.id = processors.component
	JOIN
		processor_joins on processor_joins.processor = processors.id
	JOIN
		topics on topics.id = processor_joins.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, groupName, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &groupName, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan topic input")
		}

		p := getProcessor(componentName, name, description, groupName)

		p.Joins = append(p.Joins, &discoveryv1.Join{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	// lookups
	rows, err = r.db.QueryContext(ctx, `
	SELECT
		processors.name,
		processors.description,
		processors.group_name,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_processors on pods.id = pod_processors.pod
	JOIN
		processors on processors.id = pod_processors.processor
	JOIN
		components on components.id = processors.component
	JOIN
		processor_lookups on processor_lookups.processor = processors.id
	JOIN
		topics on topics.id = processor_lookups.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, groupName, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &groupName, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan topic input")
		}

		p := getProcessor(componentName, name, description, groupName)

		p.Lookups = append(p.Lookups, &discoveryv1.Lookup{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	// outputs
	rows, err = r.db.QueryContext(ctx, `
	SELECT
		processors.name,
		processors.description,
		processors.group_name,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_processors on pods.id = pod_processors.pod
	JOIN
		processors on processors.id = pod_processors.processor
	JOIN
		components on components.id = processors.component
	JOIN
		processor_outputs on processor_outputs.processor = processors.id
	JOIN
		topics on topics.id = processor_outputs.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, groupName, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &groupName, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan topic input")
		}

		p := getProcessor(componentName, name, description, groupName)

		p.Outputs = append(p.Outputs, &discoveryv1.Output{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	//persistence
	rows, err = r.db.QueryContext(ctx, `
	SELECT
		processors.name,
		processors.description,
		processors.group_name,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_processors on pods.id = pod_processors.pod
	JOIN
		processors on processors.id = pod_processors.processor
	JOIN
		components on components.id = processors.component
	JOIN
		topics on topics.id = processors.persistence
	WHERE
		pods.name=$1 and
		processors.persistence is not null
	`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, groupName, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &groupName, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan topic input")
		}

		p := getProcessor(componentName, name, description, groupName)

		p.Persistence = &discoveryv1.Persistence{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		}
	}

	for _, component := range service.Components {
		processors, ok := componentProcessors[component.Name]
		if !ok {
			continue
		}
		for _, processor := range processors {
			component.Processors = append(component.Processors, processor)
		}
	}

	return nil
}

func (r *Retriever) getSources(ctx context.Context, service *discoveryv1.Service, pod string) error {
	sources := map[string][]*discoveryv1.Source{}

	rows, err := r.db.QueryContext(ctx, `
	SELECT
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_sources on pods.id = pod_sources.pod
	JOIN
		sources on sources.id = pod_sources.source
	JOIN
		components on components.id = sources.component
	JOIN
		topics on topics.id = sources.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var topicName, topicMessage, componentName string
		err = rows.Scan(&topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan source")
		}

		_, ok := sources[componentName]
		if !ok {
			sources[componentName] = []*discoveryv1.Source{}
		}

		sources[componentName] = append(sources[componentName], &discoveryv1.Source{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	for _, component := range service.Components {
		sources, ok := sources[component.Name]
		if !ok {
			continue
		}

		component.Sources = sources
	}

	return nil
}

func (r *Retriever) getViews(ctx context.Context, service *discoveryv1.Service, pod string) error {
	views := map[string][]*discoveryv1.View{}

	rows, err := r.db.QueryContext(ctx, `
	SELECT
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_views on pods.id = pod_views.pod
	JOIN
		views on views.id = pod_views.view
	JOIN
		components on components.id = views.component
	JOIN
		topics on topics.id = views.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var topicName, topicMessage, componentName string
		err = rows.Scan(&topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan source")
		}

		_, ok := views[componentName]
		if !ok {
			views[componentName] = []*discoveryv1.View{}
		}

		views[componentName] = append(views[componentName], &discoveryv1.View{
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	for _, component := range service.Components {
		views, ok := views[component.Name]
		if !ok {
			continue
		}

		component.Views = views
	}

	return nil
}

func (r *Retriever) getSinks(ctx context.Context, service *discoveryv1.Service, pod string) error {
	sinks := map[string][]*discoveryv1.Sink{}

	rows, err := r.db.QueryContext(ctx, `
	SELECT
		sinks.name as sink_name,
		sinks.description as sink_description,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_sinks on pods.id = pod_sinks.pod
	JOIN
		sinks on sinks.id = pod_sinks.sink
	JOIN
		components on components.id = sinks.component
	JOIN
		topics on topics.id = sinks.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan source")
		}

		_, ok := sinks[componentName]
		if !ok {
			sinks[componentName] = []*discoveryv1.Sink{}
		}

		sinks[componentName] = append(sinks[componentName], &discoveryv1.Sink{
			Name:        name,
			Description: description,
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	for _, component := range service.Components {
		sinks, ok := sinks[component.Name]
		if !ok {
			continue
		}

		component.Sinks = sinks
	}

	return nil
}

func (r *Retriever) getViewSinks(ctx context.Context, service *discoveryv1.Service, pod string) error {
	sinks := map[string][]*discoveryv1.ViewSink{}

	rows, err := r.db.QueryContext(ctx, `
	SELECT
		view_sinks.name as sink_name,
		view_sinks.description as sink_description,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_view_sinks on pods.id = pod_view_sinks.pod
	JOIN
		view_sinks on view_sinks.id = pod_view_sinks.view_sink
	JOIN
		components on components.id = view_sinks.component
	JOIN
		topics on topics.id = view_sinks.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan source")
		}

		_, ok := sinks[componentName]
		if !ok {
			sinks[componentName] = []*discoveryv1.ViewSink{}
		}

		sinks[componentName] = append(sinks[componentName], &discoveryv1.ViewSink{
			Name:        name,
			Description: description,
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	for _, component := range service.Components {
		sinks, ok := sinks[component.Name]
		if !ok {
			continue
		}

		component.ViewSinks = sinks
	}

	return nil
}

func (r *Retriever) getViewSources(ctx context.Context, service *discoveryv1.Service, pod string) error {
	sources := map[string][]*discoveryv1.ViewSource{}

	rows, err := r.db.QueryContext(ctx, `
	SELECT
		view_sources.name as sink_name,
		view_sources.description as sink_description,
		topics.name as topic_name,
		topics.message as topic_message,
		components.name as component_name
	FROM
		pods
	JOIN
		pod_view_sources on pods.id = pod_view_sources.pod
	JOIN
		view_sources on view_sources.id = pod_view_sources.view_source
	JOIN
		components on components.id = view_sources.component
	JOIN
		topics on topics.id = view_sources.topic
	WHERE
		pods.name=$1`, pod)
	if err != nil {
		return errors.Wrap(err, "failed to query processorss")
	}
	defer rows.Close()

	for rows.Next() {
		var name, description, topicName, topicMessage, componentName string
		err = rows.Scan(&name, &description, &topicName, &topicMessage, &componentName)
		if err != nil {
			return errors.Wrap(err, "failed to scan source")
		}

		_, ok := sources[componentName]
		if !ok {
			sources[componentName] = []*discoveryv1.ViewSource{}
		}

		sources[componentName] = append(sources[componentName], &discoveryv1.ViewSource{
			Name:        name,
			Description: description,
			Topic: &discoveryv1.TopicDefinition{
				Topic:   topicName,
				Message: topicMessage,
			},
		})
	}

	for _, component := range service.Components {
		sources, ok := sources[component.Name]
		if !ok {
			continue
		}

		component.ViewSources = sources
	}

	return nil
}
