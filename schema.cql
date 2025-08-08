CREATE KEYSPACE IF NOT EXISTS wikipedia 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS wikipedia.page_creations (
    user_id int,
    domain text,
    created_at timestamp,
    page_title text,
    PRIMARY KEY ((domain), created_at, user_id)
) WITH CLUSTERING ORDER BY (created_at DESC);