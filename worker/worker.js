const { Client } = require('pg');
const Redis = require('ioredis');
const crypto = require('crypto');
const dotenv = require('dotenv');

dotenv.config();

// Database and Redis Configuration derived purely from environment variables
const dbConfig = {
    host: process.env.POSTGRES_HOST,
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
};

const redisUrl = process.env.REDIS_URL;

let pgClient;
let redis;
let redisSub; // Dedicated client for Pub/Sub blocking

// Lua Script for Atomic Claiming from Redis Sorted Set
const CLAIM_LUA_SCRIPT = `
    local set_key = KEYS[1]
    local current_time = tonumber(ARGV[1])
    
    -- Get the top element where score <= current_time
    local result = redis.call('ZRANGEBYSCORE', set_key, 0, current_time, 'LIMIT', 0, 1)
    
    if #result > 0 then
        local event_id = result[1]
        redis.call('ZREM', set_key, event_id) -- Remove instantly so no other worker grabs it
        return event_id
    end
    return nil
`;

// --- 1. DEFEAT DOCKER-COMPOSE RACE TRAP: Connection Retry with Exponential Backoff ---
async function initializeConnections() {
    let retries = 5;
    let delay = 1000;

    while (retries > 0) {
        try {
            console.log(`[${WORKER_ID}] Attempting infrastructure connections...`);
            
            pgClient = new Client(dbConfig);
            await pgClient.connect();
            
            redis = new Redis(redisUrl);
            redisSub = new Redis(redisUrl);
            
            // Define custom command for our Lua script
            redis.defineCommand('claimEvent', {
                numberOfKeys: 1,
                lua: CLAIM_LUA_SCRIPT
            });

            await redisSub.subscribe(INTERRUPT_CHANNEL);
            console.log(`[${WORKER_ID}] Connected to PostgreSQL and Redis successfully.`);
            return;
        } catch (error) {
            console.error(`[${WORKER_ID}] Connection failed. Retries remaining: ${retries - 1}. Error:`, error.message);
            retries--;
            if (retries === 0) {
                console.error(`[${WORKER_ID}] Could not connect to infrastructure. Exiting.`);
                process.exit(1);
            }
            await new Promise(res => setTimeout(res, delay));
            delay *= 2; // Exponential backoff
        }
    }
}

// --- 2. DEFEAT CLOCK SKEW: Use Central Database Time as Source of Truth ---
// async function getCentralTimeMs() {
//     const res = await pgClient.query("SELECT EXTRACT(EPOCH FROM NOW()) * 1000 AS ts;");
//     return Math.floor(parseFloat(res.rows[0].ts));
// }
// --- OPTIMIZED CLOCK SYNC ---
// Calculates the drift delta relative to the central DB clock
async function synchronizeCentralClock() {
    try {
        const start = Date.now();
        const res = await pgClient.query("SELECT EXTRACT(EPOCH FROM NOW()) * 1000 AS ts;");
        const lat = (Date.now() - start) / 2; // Account for network roundtrip latency
        const dbTime = Math.floor(parseFloat(res.rows[0].ts)) + lat;
        
        clockOffset = dbTime - Date.now();
    } catch (err) {
        console.error(`[${WORKER_ID}] Clock sync failure:`, err.message);
    }
}
// --- 3. DEFEAT POLLING TRAP: Dynamic Sleep with Pub/Sub Interrupts ---
function sleepOrInterrupt(ms) {
    if (ms <= 0) return Promise.resolve({ interrupted: false });
    
    return new Promise((resolve) => {
        let timeoutToken;

        const messageHandler = (channel) => {
            if (channel === INTERRUPT_CHANNEL) {
                clearTimeout(timeoutToken);
                redisSub.off('message', messageHandler);
                resolve({ interrupted: true });
            }
        };

        timeoutToken = setTimeout(() => {
            redisSub.off('message', messageHandler);
            resolve({ interrupted: false });
        }, ms);

        redisSub.on('message', messageHandler);
    });
}