CREATE TABLE processed_events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    processor_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX processed_events_user_seq_idx ON processed_events (user_id, seq);

CREATE TABLE dedup_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dropped_duplicates BIGINT NOT NULL DEFAULT 0
);

INSERT INTO dedup_stats (id, dropped_duplicates) VALUES (1, 0);
