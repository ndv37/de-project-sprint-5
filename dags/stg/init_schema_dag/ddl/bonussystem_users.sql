CREATE TABLE IF NOT EXISTS stg.bonussystem_users
(
    id integer CONSTRAINT bonussystem_users_pkey PRIMARY KEY,
    order_user_id text NOT NULL
);