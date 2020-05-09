CREATE VIEW component_topic_dependencies AS
    select
        components.id as component,
        views.topic
    from
        components
    inner join
        views on views.component=components.id

    union

    select
        components.id as component,
        view_sinks.topic
    from
        components
    inner join
        view_sinks on view_sinks.component=components.id

    union

    select
        components.id as component,
        sinks.topic
    from
        components
    inner join
        sinks on sinks.component=components.id
        
    union

    select
        components.id as component,
        processor_inputs.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_inputs on processor_inputs.processor=processors.id
        
    union

    select
        components.id as component,
        processor_lookups.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_lookups on processor_lookups.processor=processors.id

    union

    select
        components.id as component,
        processor_joins.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_joins on processor_joins.processor=processors.id;

CREATE VIEW component_topic_sources AS
    select
        components.id as component,
        sources.topic
    from
        components
    inner join
        sources on sources.component=components.id

    union

    select
        components.id as component,
        view_sources.topic
    from
        components
    inner join 
        view_sources on view_sources.component=components.id

    union

    select
        components.id as component,
        processor_outputs.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_outputs on processor_outputs.processor=processors.id
        
    union

    select
        components.id as component,
        processors.persistence as topic
    from
        components
    inner join
        processors on processors.component=components.id
    where
        processors.persistence is not null
