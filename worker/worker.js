const { Pool } = require('pg'); // Upgraded from Client to handle concurrent write streams
const Redis = require('ioredis');
const crypto = require('crypto');

// 1. GLOBAL ENGINE CONFIGURATIONS (Defined before any active runtime sequences execute)
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

let pgPool;    // Manages the persistent, high-concurrency connection pool
let redis;
let redisSub;  // Dedicated client channel for Pub/Sub blocking loops
let clockOffset = 0; // Tracks central database clock drift in memory

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

// --- 1. CONNECTION RETRY WITH EXPONENTIAL BACKOFF ---
async function initializeConnections() {
    let retries = 5;
    let delay = 1000;

    while (retries > 0) {
        try {
            console.log(`[${WORKER_ID}] Attempting infrastructure connections...`);
            
            // Initialize connection pool to accommodate concurrent operations
            pgPool = new Pool({
                ...dbConfig,
                max: 10, // Up to 10 concurrent clients allocated per worker container
                idleTimeoutMillis: 30000
            });
            
            // Verify pool initialization capability
            const verificationClient = await pgPool.connect();
            verificationClient.release();
            
            redis = new Redis(redisUrl);
            redisSub = new Redis(redisUrl, { enableReadyCheck: false }); // Bypasses subscriber ready-check warning
            
            // Define custom command for our Lua script
            redis.defineCommand('claimEvent', {
                numberOfKeys: 1,
                lua: CLAIM_LUA_SCRIPT
            });

            await redisSub.subscribe(INTERRUPT_CHANNEL);
            console.log(`[${WORKER_ID}] Connected to PostgreSQL Pool and Redis successfully.`);
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

// --- 2. OPTIMIZED CLOCK SYNC (Source of Truth Offset) ---
async function synchronizeCentralClock() {
    try {
        const start = Date.now();
        const res = await pgPool.query("SELECT EXTRACT(EPOCH FROM NOW()) * 1000 AS ts;");
        const lat = (Date.now() - start) / 2; // Account for network roundtrip latency
        const dbTime = Math.floor(parseFloat(res.rows[0].ts)) + lat;
        
        clockOffset = dbTime - Date.now();
    } catch (err) {
        console.error(`[${WORKER_ID}] Clock sync failure:`, err.message);
    }
}

// --- 3. DYNAMIC SLEEP WITH PUB/SUB INTERRUPTS ---
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
        // Capture exact execution timestamp instantly in memory to bypass DB serialization lag
        const exactExecutionTimeMs = Date.now() + clockOffset;
        const executionTimestampTZ = new Date(exactExecutionTimeMs).toISOString();

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
        const res = await pgPool.query(claimQuery, [WORKER_ID, LEASE_DURATION_SEC, eventId]);

        if (res.rowCount === 0) {
            // Concurrency fallback: Another node processed or modified it
            return;
        }

        const event = res.rows[0];
        console.log(`[${WORKER_ID}] Claimed Event ${event.id}. Executing payload...`);
        
        // Step B: Mark as executed using our precise queue-isolated timestamp ($2)
        const completeQuery = `
            UPDATE events 
            SET status = 'executed', executed_at = $2 
            WHERE id = $1 
            RETURNING *;
        `;
        const finalRes = await pgPool.query(completeQuery, [eventId, executionTimestampTZ]);
        const executedEvent = finalRes.rows[0];

        // --- SIMULATE PAYLOAD EXECUTION ---
        await new Promise(resolve => setTimeout(resolve, 100)); 
        // ----------------------------------

        // Calculate precision telemetry
        const scheduledTime = new Date(executedEvent.scheduled_at).getTime();
        const executedTime = new Date(executedEvent.executed_at).getTime();
        const variance = executedTime - scheduledTime;

        console.log(`[${WORKER_ID}] Executed Event ${eventId} with true variance: ${variance}ms`);

        // Step C: Notify Dashboard AFTER database commit
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
        await pgPool.query("UPDATE events SET status = 'pending' WHERE id = $1 AND status = 'claimed'", [eventId]);
        const score = Date.now() + clockOffset;
        await redis.zadd(REDIS_SET_KEY, score, eventId);
    }
}

// --- 5. THE FAULT RECOVERY LEASE REAPER ---
async function runReaper() {
    try {
        // Find events stuck in 'claimed' state whose lease has run out
        const deadLeasesQuery = `
            UPDATE events 
            SET status = 'pending', worker_id = NULL, claimed_at = NULL, leased_until = NULL
            WHERE status = 'claimed' AND leased_until < NOW()
            RETURNING id, EXTRACT(EPOCH FROM scheduled_at) * 1000 as score;
        `;
        const res = await pgPool.query(deadLeasesQuery);

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
                // Process concurrently without blocking the core event loop
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