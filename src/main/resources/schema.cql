DROP KEYSPACE IF EXISTS rinha;

CREATE KEYSPACE IF NOT EXISTS rinha WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS rinha.transactions (
    accountId int,
    type text,
    description text,
    date timestamp,
    amount int,
    dateMillis bigint,
    lastStatementBalance int,
    PRIMARY KEY ((accountId), dateMillis)
) WITH CLUSTERING ORDER BY (dateMillis DESC);