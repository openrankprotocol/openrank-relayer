-- todo adjust types

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    type VARCHAR NOT NULL,
    job_seq_number INTEGER NOT NULL UNIQUE,
    body JSONB NOT NULL,
    "from" VARCHAR,
    "to" VARCHAR,
    hash VARCHAR NOT NULL,
    internal_id VARCHAR NOT NULL UNIQUE,
    provider VARCHAR
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

CREATE INDEX idx_transactions_hash ON transactions USING HASH (hash);
