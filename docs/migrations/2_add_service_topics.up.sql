CREATE VIEW service_topic_dependencies AS
    select
        components.service,
        views.topic
    from
        components
    inner join
        views on views.component=components.id

    union

    select
        components.service,
        view_sinks.topic
    from
        components
    inner join
        view_sinks on view_sinks.component=components.id

    union

    select
        components.service,
        sinks.topic
    from
        components
    inner join
        sinks on sinks.component=components.id
        
    union

    select
        components.service,
        processor_inputs.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_inputs on processor_inputs.processor=processors.id
        
    union

    select
        components.service,
        processor_lookups.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_lookups on processor_lookups.processor=processors.id

    union

    select
        components.service,
        processor_joins.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_joins on processor_joins.processor=processors.id;

CREATE VIEW service_topic_sources AS
    select
        components.service,
        sources.topic
    from
        components
    inner join
        sources on sources.component=components.id

    union

    select
        components.service,
        view_sources.topic
    from
        components
    inner join 
        view_sources on view_sources.component=components.id

    union

    select
        components.service,
        processor_outputs.topic
    from
        components
    inner join
        processors on processors.component=components.id
    inner join
        processor_outputs on processor_outputs.processor=processors.id
        
    union

    select
        components.service,
        processors.persistence
    from
        components
    inner join
        processors on processors.component=components.id
    where
        processors.persistence is not null
