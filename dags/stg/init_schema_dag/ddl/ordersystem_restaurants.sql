CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

ALTER TABLE stg.ordersystem_restaurants ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);