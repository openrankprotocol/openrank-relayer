-- todo adjust types

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR,
    event_id VARCHAR NOT NULL UNIQUE,
    event_body JSONB NOT NULL,
    "from" VARCHAR,
    "to" VARCHAR,
    hash VARCHAR,
    provider VARCHAR,
    compute VARCHAR
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY, 
    transaction_hashes VARCHAR[] NOT NULL, 
    seq_number INTEGER NOT NULL UNIQUE
);

CREATE INDEX idx_jobs_seq_number ON jobs (seq_number);

CREATE TABLE state (
    id SERIAL PRIMARY KEY,
    key_name VARCHAR UNIQUE NOT NULL,
    last_processed_key INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_event_id_hash ON events USING HASH (event_id);
CREATE INDEX idx_event_hash ON events USING HASH (hash);
