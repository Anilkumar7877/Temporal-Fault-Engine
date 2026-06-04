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