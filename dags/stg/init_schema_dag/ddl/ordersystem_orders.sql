CREATE TABLE if not exists stg.ordersystem_orders
(
    id serial CONSTRAINT ordersystem_orders_pkey PRIMARY KEY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT null
);