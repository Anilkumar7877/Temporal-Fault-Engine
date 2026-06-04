const { Client } = require('pg');
const Redis = require('ioredis');
const crypto = require('crypto');
const dotenv = require('dotenv');

// 1. GLOBAL ENGINE CONFIGURATIONS (Must be defined before any functions run)
const WORKER_ID = process.env.HOSTNAME || `worker-${crypto.randomUUID().substring(0, 8)}`;
const LEASE_DURATION_SEC = 5;
const REDIS_SET_KEY = 'scheduled_events';
const INTERRUPT_CHANNEL = 'event_interrupt';

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

// --- 4. EXACTLY-ONCE EXECUTION: Processing Logic ---
async function processEvent(eventId) {
    try {
        // Step A: Attempt to claim lease in Postgres
        const claimQuery = `
            UPDATE events 
            SET status = 'claimed', 
                worker_id = $1, 
                claimed_at = NOW(), 
                leased_until = NOW() + cast($2 || ' second' as INTERVAL)
            WHERE id = $3 AND status = 'pending'
            RETURNING *;
        `;
        const res = await pgClient.query(claimQuery, [WORKER_ID, LEASE_DURATION_SEC, eventId]);

        if (res.rowCount === 0) {
            // Concurrency fallback: Another node processed or modified it
            return;
        }

        const event = res.rows[0];
        console.log(`[${WORKER_ID}] Claimed Event ${event.id}. Executing payload...`);
        
        // Step B: Mark as executed
        const completeQuery = `
            UPDATE events 
            SET status = 'executed', executed_at = NOW() 
            WHERE id = $1 
            RETURNING *;
        `;
        const finalRes = await pgClient.query(completeQuery, [eventId]);
        const executedEvent = finalRes.rows[0];

        // --- SIMULATE PAYLOAD EXECUTION ---
        // Replace this block with your actual execution task logic
        await new Promise(resolve => setTimeout(resolve, 100)); 
        // ----------------------------------

        // Calculate precision telemetry
        const scheduledTime = new Date(executedEvent.scheduled_at).getTime();
        const executedTime = new Date(executedEvent.executed_at).getTime();
        const variance = executedTime - scheduledTime;

        console.log(`[${WORKER_ID}] Executed Event ${eventId} with variance: ${variance}ms`);

        // Step C: DEFEAT NOTIFICATION-BEFORE-COMMIT: Notify Dashboard *AFTER* database commit
        const dashboardPayload = {
            id: executedEvent.id,
            payload: executedEvent.payload,
            scheduled_at: scheduledTime,
            executed_at: executedTime,
            variance: variance,
            worker_id: WORKER_ID
        };
        await redis.publish('dashboard_execution_stream', JSON.stringify(dashboardPayload));

    } catch (error) {
        console.error(`[${WORKER_ID}] Error processing event ${eventId}:`, error);
        // Fallback status reset on explicit error
        await pgClient.query("UPDATE events SET status = 'pending' WHERE id = $1 AND status = 'claimed'", [eventId]);
        const score = Date.now() + clockOffset;
        await redis.zadd(REDIS_SET_KEY, score, eventId);
    }
}

// --- 5. DEFEAT KILL -9 TRAP: The Fault Recovery Lease Reaper ---
async function runReaper() {
    try {
        // Find events stuck in 'claimed' state whose lease has run out
        const deadLeasesQuery = `
            UPDATE events 
            SET status = 'pending', worker_id = NULL, claimed_at = NULL, leased_until = NULL
            WHERE status = 'claimed' AND leased_until < NOW()
            RETURNING id, EXTRACT(EPOCH FROM scheduled_at) * 1000 as score;
        `;
        const res = await pgClient.query(deadLeasesQuery);

        for (const row of res.rows) {
            console.warn(`[${WORKER_ID} REAPER] Detected crashed worker lease for Event ${row.id}. Re-indexing.`);
            // Push back into the Redis scheduling engine instantly
            await redis.zadd(REDIS_SET_KEY, Math.floor(row.score), row.id);
            // Alert other workers to re-evaluate loops
            await redis.publish(INTERRUPT_CHANNEL, 'requeue');
        }
    } catch (error) {
        console.error(`[${WORKER_ID} REAPER] Execution error:`, error.message);
    } finally {
        setTimeout(runReaper, 2000); // Poll for dead containers every 2 seconds
    }
}

// --- MAIN ENGINE WORKER LOOP ---
async function startEngineLoop() {
    // Run an initial clock synchronization
    await synchronizeCentralClock();
    
    // Periodically refresh the clock offset every 10 seconds in the background
    setInterval(synchronizeCentralClock, 10000);
    while (true) {
        try {
            const currentCentralTime = Date.now() + clockOffset;

            // Try to pull an execution target using our atomic Lua script
            const eventId = await redis.claimEvent(REDIS_SET_KEY, currentCentralTime);

            if (eventId) {
                // REMOVE THE 'await' HERE to process concurrently without blocking the loop!
                processEvent(eventId).catch(err => 
                    console.error(`[${WORKER_ID}] Async processing error:`, err.message)
                );
                continue; 
            }

            // No events due right now. Find out when the next closest event is.
            const nextEvent = await redis.zrange(REDIS_SET_KEY, 0, 0, 'WITHSCORES');

            if (nextEvent.length === 0) {
                console.log(`[${WORKER_ID}] Queue empty. Entering deep sleep...`);
                await sleepOrInterrupt(3600000); 
            } else {
                const nextEventTime = parseInt(nextEvent[1]);
                const sleepDuration = nextEventTime - currentCentralTime;

                if (sleepDuration > 0) {
                    console.log(`[${WORKER_ID}] Next event in ${sleepDuration}ms. Sleeping...`);
                    const status = await sleepOrInterrupt(sleepDuration);
                    if (status.interrupted) {
                        console.log(`[${WORKER_ID}] Sleep interrupted by incoming high-priority event. Re-evaluating...`);
                    }
                }
            }
        } catch (error) {
            console.error(`[${WORKER_ID} LOOP] Fatal processing step context error:`, error.message);
            await new Promise(res => setTimeout(res, 1000)); 
        }
    }
}

// --- INITIALIZE EXECUTION LAYER ---
(async () => {
    await initializeConnections();
    
    // Fire up the safety lease checker engine
    runReaper();
    
    // Execute core loop engine
    startEngineLoop();
})();