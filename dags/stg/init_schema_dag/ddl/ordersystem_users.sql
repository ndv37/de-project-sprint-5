CREATE TABLE if not exists stg.ordersystem_users
(
    id serial CONSTRAINT ordersystem_users_pkey PRIMARY KEY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT null,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id)
);
