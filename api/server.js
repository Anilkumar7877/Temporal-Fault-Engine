const express = require('express');
const { Client } = require('pg');
const Redis = require('ioredis');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'frontend')));

const PORT = process.env.PORT || 3000;
const INTERRUPT_CHANNEL = 'event_interrupt';
const REDIS_SET_KEY = 'scheduled_events';

// Ingress Database & Redis Configurations derived strictly from environment variables
const dbConfig = {
    host: process.env.POSTGRES_HOST,
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
};
const redisUrl = process.env.REDIS_URL;

let pgClient;
let redisPub;
let redisSub;
let clockOffset = 0; // Tracks central database clock drift in memory

// Background clock sync pattern to eliminate database query roundtrips on ingestion
async function synchronizeCentralClock() {
    try {
        const start = Date.now();
        const res = await pgClient.query("SELECT EXTRACT(EPOCH FROM NOW()) * 1000 AS ts;");
        const roundTripLatency = (Date.now() - start) / 2;
        const dbTime = Math.floor(parseFloat(res.rows[0].ts)) + roundTripLatency;
        clockOffset = dbTime - Date.now();
    } catch (err) {
        console.error('API clock synchronization failure:', err.message);
    }
}

// Connect with container boot retry logic
async function initStore() {
    try {
        pgClient = new Client(dbConfig);
        await pgClient.connect();
        
        redisPub = new Redis(redisUrl);
        redisSub = new Redis(redisUrl, { enableReadyCheck: false });
        
        // Initial clock calculation + establish a 10-second background heartbeat refresh
        await synchronizeCentralClock();
        setInterval(synchronizeCentralClock, 10000);
        
        console.log('API Gateway safely connected to persistent stores and synced centralized time.');
        
        // Setup pub/sub listener for the real-time dashboard stream
        await redisSub.subscribe('dashboard_execution_stream');
        redisSub.on('message', (channel, message) => {
            if (channel === 'dashboard_execution_stream') {
                broadcastToDashboard(JSON.parse(message));
            }
        });
    } catch (err) {
        console.error('API Store connection delayed, retrying in 2s...', err.message);
        setTimeout(initStore, 2000);
    }
}
initStore();

// --- HTTP ENDPOINT: SCHEDULE EVENT ---
app.post('/schedule', async (req, res) => {
    const { payload, delayMs } = req.body;

    if (!payload || typeof delayMs !== 'number') {
        return res.status(400).json({ error: 'Missing required payload or delayMs fields' });
    }

    try {
        const eventId = crypto.randomUUID();
        
        // Compute the precise target timestamp instantly using the in-memory offset
        const currentCentralTime = Date.now() + clockOffset;
        const targetExecutionTime = currentCentralTime + delayMs;
        const targetTimestampTZ = new Date(targetExecutionTime).toISOString();

        // 1. Commit to PostgreSQL Source of Truth
        await pgClient.query(
            "INSERT INTO events (id, payload, scheduled_at, status) VALUES ($1, $2, $3, 'pending')",
            [eventId, JSON.stringify(payload), targetTimestampTZ]
        );

        // 2. Index into Redis Sorted Set
        await redisPub.zadd(REDIS_SET_KEY, targetExecutionTime, eventId);

        // 3. Alert workers to instantly re-evaluate loop dynamic sleep timers
        await redisPub.publish(INTERRUPT_CHANNEL, 'new_event');

        return res.status(202).json({
            id: eventId,
            status: 'pending',
            scheduled_at: targetTimestampTZ,
            delay_ms: delayMs
        });
    } catch (error) {
        console.error('Scheduling error:', error);
        return res.status(500).json({ error: 'Internal server failure saving event' });
    }
});

// --- HTTP ENDPOINT: CLEAR ALL EVENTS ---
app.post('/clear', async (req, res) => {
    try {
        // 1. Truncate PostgreSQL events table
        await pgClient.query("TRUNCATE TABLE events RESTART IDENTITY;");

        // 2. Clear Redis Sorted Set
        await redisPub.del(REDIS_SET_KEY);

        // 3. Alert workers to instantly re-evaluate loop dynamic sleep timers
        await redisPub.publish(INTERRUPT_CHANNEL, 'clear');

        return res.status(200).json({ message: 'All events successfully cleared from PostgreSQL and Redis' });
    } catch (error) {
        console.error('Clear events error:', error);
        return res.status(500).json({ error: 'Internal server failure clearing events' });
    }
});

// GET /metrics - Prometheus Observability Endpoint
app.get('/metrics', async (req, res) => {
    try {
        const query = `
            SELECT 
                COUNT(*) as scheduled,
                COUNT(*) FILTER (WHERE status = 'executed') as executed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COALESCE(SUM(EXTRACT(EPOCH FROM (executed_at - scheduled_at)) * 1000) FILTER (WHERE status = 'executed'), 0) as variance_sum,
                COUNT(*) FILTER (WHERE status = 'executed' AND EXTRACT(EPOCH FROM (executed_at - scheduled_at)) * 1000 <= 5) as b_5,
                COUNT(*) FILTER (WHERE status = 'executed' AND EXTRACT(EPOCH FROM (executed_at - scheduled_at)) * 1000 <= 15) as b_15,
                COUNT(*) FILTER (WHERE status = 'executed' AND EXTRACT(EPOCH FROM (executed_at - scheduled_at)) * 1000 <= 50) as b_50,
                COUNT(*) FILTER (WHERE status = 'executed' AND EXTRACT(EPOCH FROM (executed_at - scheduled_at)) * 1000 <= 200) as b_200
            FROM events;
        `;
        
        const dbRes = await pgClient.query(query);
        const m = dbRes.rows[0];

        // Format metrics explicitly into standard Prometheus exposition format
        const responseText = [
            `# HELP scheduler_events_scheduled_total Total events submitted to the system.`,
            `# TYPE scheduler_events_scheduled_total counter`,
            `scheduler_events_scheduled_total ${m.scheduled}`,
            ``,
            `# HELP scheduler_events_executed_total Total events successfully executed by workers.`,
            `# TYPE scheduler_events_executed_total counter`,
            `scheduler_events_executed_total ${m.executed}`,
            ``,
            `# HELP scheduler_events_failed_total Total events that permanently failed execution boundaries.`,
            `# TYPE scheduler_events_failed_total counter`,
            `scheduler_events_failed_total ${m.failed}`,
            ``,
            `# HELP scheduler_execution_variance_milliseconds Histogram tracking execution accuracy lag.`,
            `# TYPE scheduler_execution_variance_milliseconds histogram`,
            `scheduler_execution_variance_milliseconds_bucket{le="5"} ${m.b_5}`,
            `scheduler_execution_variance_milliseconds_bucket{le="15"} ${m.b_15}`,
            `scheduler_execution_variance_milliseconds_bucket{le="50"} ${m.b_50}`,
            `scheduler_execution_variance_milliseconds_bucket{le="200"} ${m.b_200}`,
            `scheduler_execution_variance_milliseconds_bucket{le="+Inf"} ${m.executed}`,
            `scheduler_execution_variance_milliseconds_sum ${parseFloat(m.variance_sum).toFixed(2)}`,
            `scheduler_execution_variance_milliseconds_count ${m.executed}`
        ].join('\n');

        // Set mandatory Prometheus text content headers
        res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
        res.status(200).send(responseText);

    } catch (err) {
        console.error('Metrics aggregation failure:', err.message);
        res.status(500).send('# ERROR: Failed to gather engine cluster telemetry metrics');
    }
});

// Start HTTP Server
const server = app.listen(PORT, () => {
    console.log(`Ingress HTTP Server running on port ${PORT}`);
});

// --- WEBSOCKET SERVER: REAL-TIME TELEMETRY STREAM ---
const wss = new WebSocketServer({ server });
const clients = new Set();

wss.on('connection', (ws) => {
    clients.add(ws);
    ws.on('close', () => clients.delete(ws));
});

function broadcastToDashboard(data) {
    const messageStr = JSON.stringify(data);
    for (const client of clients) {
        if (client.readyState === 1) { // Open state
            client.send(messageStr);
        }
    }
}