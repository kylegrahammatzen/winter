package queue

import "github.com/redis/go-redis/v9"

var enqueueScript = redis.NewScript(`
local jobKey = KEYS[1]
local readyKey = KEYS[2]
local delayedKey = KEYS[3]
local statsKey = KEYS[4]
local uniqueKey = KEYS[5]

local id = ARGV[1]
local kind = ARGV[2]
local queue = ARGV[3]
local priority = tonumber(ARGV[4])
local state = ARGV[5]
local payload = ARGV[6]
local maxRetries = tonumber(ARGV[7])
local createdAt = ARGV[8]
local scheduledAt = tonumber(ARGV[9])
local uniqueTTL = tonumber(ARGV[10])

if uniqueKey ~= "" and uniqueTTL > 0 then
    local set = redis.call("SET", uniqueKey, id, "NX", "EX", uniqueTTL)
    if not set then
        return redis.error_reply("DUPLICATE")
    end
end

redis.call("HSET", jobKey,
    "id", id,
    "kind", kind,
    "queue", queue,
    "priority", priority,
    "state", state,
    "payload", payload,
    "attempt", 0,
    "max_retries", maxRetries,
    "created_at", createdAt,
    "scheduled_at", scheduledAt,
    "started_at", 0,
    "completed_at", 0,
    "last_error", "")

if scheduledAt > 0 then
    redis.call("ZADD", delayedKey, scheduledAt, id)
else
    redis.call("ZADD", readyKey, priority, id)
end

redis.call("HINCRBY", statsKey, "enqueued", 1)
return id
`)

var dequeueScript = redis.NewScript(`
local readyKey = KEYS[1]
local activeKey = KEYS[2]
local pausedKey = KEYS[3]
local workerJobsKey = KEYS[4]

local workerID = ARGV[1]
local now = ARGV[2]

if redis.call("EXISTS", pausedKey) == 1 then
    return nil
end

local result = redis.call("ZPOPMIN", readyKey, 1)
if #result == 0 then
    return nil
end

local id = result[1]
local jobKey = "winter:job:" .. id

redis.call("SADD", activeKey, id)
redis.call("HSET", jobKey, "state", "active", "started_at", now)
redis.call("SADD", workerJobsKey, id)

local data = redis.call("HGETALL", jobKey)
return data
`)

var ackScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local statsKey = KEYS[3]
local workerJobsKey = KEYS[4]

local id = ARGV[1]
local now = ARGV[2]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("HSET", jobKey, "state", "completed", "completed_at", now)
redis.call("HINCRBY", statsKey, "completed", 1)
return 1
`)

var nackScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local delayedKey = KEYS[3]
local deadKey = KEYS[4]
local statsKey = KEYS[5]
local workerJobsKey = KEYS[6]

local id = ARGV[1]
local errorMsg = ARGV[2]
local backoffMs = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local skipRetry = tonumber(ARGV[5])

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)

local attempt = redis.call("HINCRBY", jobKey, "attempt", 1)
redis.call("HSET", jobKey, "last_error", errorMsg)

local maxRetries = tonumber(redis.call("HGET", jobKey, "max_retries"))

if skipRetry == 1 or attempt >= maxRetries then
    redis.call("LPUSH", deadKey, id)
    redis.call("HSET", jobKey, "state", "dead")
    redis.call("HINCRBY", statsKey, "dead", 1)
    return "dead"
end

local nextRun = now + backoffMs
redis.call("ZADD", delayedKey, nextRun, id)
redis.call("HSET", jobKey, "state", "retry")
return "retry"
`)

var rescheduleScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local delayedKey = KEYS[3]
local workerJobsKey = KEYS[4]

local id = ARGV[1]
local newTimestamp = ARGV[2]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("ZADD", delayedKey, newTimestamp, id)
redis.call("HSET", jobKey, "state", "pending", "scheduled_at", newTimestamp)
return 1
`)

var cancelScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local workerJobsKey = KEYS[3]

local id = ARGV[1]
local now = ARGV[2]
local reason = ARGV[3]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("HSET", jobKey, "state", "cancelled", "completed_at", now, "last_error", reason)
return 1
`)

var promoteScript = redis.NewScript(`
local delayedKey = KEYS[1]
local readyKey = KEYS[2]

local now = ARGV[1]
local limit = tonumber(ARGV[2])

local jobs = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now, "LIMIT", 0, limit)
if #jobs == 0 then
    return 0
end

for _, id in ipairs(jobs) do
    redis.call("ZREM", delayedKey, id)
    local jobKey = "winter:job:" .. id
    local priority = tonumber(redis.call("HGET", jobKey, "priority"))
    redis.call("ZADD", readyKey, priority, id)
    redis.call("HSET", jobKey, "state", "pending")
end

return #jobs
`)
