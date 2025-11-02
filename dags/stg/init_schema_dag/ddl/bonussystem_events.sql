CREATE TABLE IF NOT EXISTS stg.bonussystem_events
(
    id integer CONSTRAINT bonussystem_events_pkey PRIMARY KEY,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);


CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);