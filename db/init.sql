CREATE TYPE event_status AS ENUM ('pending', 'claimed', 'executed', 'failed');

CREATE TABLE events (
    id UUID PRIMARY KEY,
    payload JSONB NOT NULL,
    status event_status DEFAULT 'pending',
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    claimed_at TIMESTAMP WITH TIME ZONE,
    executed_at TIMESTAMP WITH TIME ZONE,
    leased_until TIMESTAMP WITH TIME ZONE,
    worker_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexing scheduled_at for the database fallback/reaper processes
CREATE INDEX idx_events_scheduled_at ON events (scheduled_at) WHERE status = 'pending';
CREATE INDEX idx_events_lease ON events (leased_until) WHERE status = 'claimed';