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