CREATE TABLE topics (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR NOT NULL,
    message VARCHAR NOT NULL,
    UNIQUE(name)
);

CREATE TABLE services (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE(name)
);

CREATE TABLE components (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR NOT NULL,
    service     INT REFERENCES services(id) NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE(name, service)
);

CREATE TABLE sources (
    id          SERIAL PRIMARY KEY,
    component   INT REFERENCES components(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(component, topic)
);

CREATE TABLE processors (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    component   INT REFERENCES components(id) NOT NULL,
    group_name  VARCHAR NOT NULL,
    persistence INT REFERENCES topics(id),
    UNIQUE(name, component)
);

CREATE TABLE processor_inputs (
    id          SERIAL PRIMARY KEY,
    processor   INT REFERENCES processors(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(processor, topic)
);

CREATE TABLE processor_joins (
    id          SERIAL PRIMARY KEY,
    processor   INT REFERENCES processors(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(processor, topic)
);

CREATE TABLE processor_lookups (
    id          SERIAL PRIMARY KEY,
    processor   INT REFERENCES processors(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(processor, topic)
);

CREATE TABLE processor_outputs (
    id          SERIAL PRIMARY KEY,
    processor   INT REFERENCES processors(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(processor, topic)
);

CREATE TABLE views (
    id          SERIAL PRIMARY KEY,
    component   INT REFERENCES components(id) NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    UNIQUE(component, topic)
);

CREATE TABLE sinks (
    id          SERIAL PRIMARY KEY,
    component   INT REFERENCES components(id) NOT NULL,
    name        VARCHAR NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE(component, name)
);

CREATE TABLE view_sinks (
    id          SERIAL PRIMARY KEY,
    component   INT REFERENCES components(id) NOT NULL,
    name        VARCHAR NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE(component, name)
);

CREATE TABLE view_sources (
    id          SERIAL PRIMARY KEY,
    component   INT REFERENCES components(id) NOT NULL,
    name        VARCHAR NOT NULL,
    topic       INT REFERENCES topics(id) NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE(component, name)
);

CREATE TABLE pods (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR NOT NULL,
    UNIQUE(name)
);

CREATE TABLE pod_processors (
    id          SERIAL PRIMARY KEY,
    pod         INT REFERENCES pods(id) NOT NULL,
    processor   INT REFERENCES processors(id) NOT NULL,
    UNIQUE(pod, processor)
);

CREATE TABLE pod_sources (
    id          SERIAL PRIMARY KEY,
    pod         INT REFERENCES pods(id) NOT NULL,
    source      INT REFERENCES sources(id) NOT NULL,
    UNIQUE(pod, source)
);

CREATE TABLE pod_views (
    id          SERIAL PRIMARY KEY,
    pod         INT REFERENCES pods(id) NOT NULL,
    view        INT REFERENCES views(id) NOT NULL,
    UNIQUE(pod, view)
);

CREATE TABLE pod_sinks (
    id          SERIAL PRIMARY KEY,
    pod         INT REFERENCES pods(id) NOT NULL,
    sink        INT REFERENCES sinks(id) NOT NULL,
    UNIQUE(pod, sink)
);

CREATE TABLE pod_view_sinks (
    id              SERIAL PRIMARY KEY,
    pod             INT REFERENCES pods(id) NOT NULL,
    view_sink       INT REFERENCES view_sinks(id) NOT NULL,
    UNIQUE(pod, view_sink)
);

CREATE TABLE pod_view_sources (
    id                  SERIAL PRIMARY KEY,
    pod                 INT REFERENCES pods(id) NOT NULL,
    view_source         INT REFERENCES view_sources(id) NOT NULL,
    UNIQUE(pod, view_source)
);
