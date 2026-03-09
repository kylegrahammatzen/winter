package queue

import "github.com/redis/go-redis/v9"

// enqueueScript atomically writes the job hash and adds it to either the ready
// or delayed sorted set, with optional unique key deduplication.
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
local workflowID = ARGV[11]

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
    "last_error", "",
    "unique_key", uniqueKey,
    "workflow_id", workflowID)

if scheduledAt > 0 then
    redis.call("ZADD", delayedKey, scheduledAt, id)
else
    redis.call("ZADD", readyKey, priority, id)
end

redis.call("HINCRBY", statsKey, "enqueued", 1)
return id
`)

// dequeueScript pops the highest-priority job from the ready set, moves it to
// active, assigns it to a worker, and sets a lease expiry for recovery.
var dequeueScript = redis.NewScript(`
local readyKey = KEYS[1]
local activeKey = KEYS[2]
local pausedKey = KEYS[3]
local workerJobsKey = KEYS[4]
local leaseKey = KEYS[5]

local workerID = ARGV[1]
local now = tonumber(ARGV[2])
local leaseDurationMs = tonumber(ARGV[3])

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
redis.call("ZADD", leaseKey, now + leaseDurationMs, id)

local data = redis.call("HGETALL", jobKey)
return data
`)

// ackScript marks a job completed and cleans it from the active set, lease ZSET,
// and worker job set. Also deletes the unique key if one exists, verifying
// ownership so we do not remove a key that belongs to a newer job.
var ackScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local statsKey = KEYS[3]
local workerJobsKey = KEYS[4]
local leaseKey = KEYS[5]

local id = ARGV[1]
local now = ARGV[2]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("ZREM", leaseKey, id)
redis.call("HSET", jobKey, "state", "completed", "completed_at", now)
redis.call("HINCRBY", statsKey, "completed", 1)

local uk = redis.call("HGET", jobKey, "unique_key")
if uk and uk ~= "" then
    local owner = redis.call("GET", uk)
    if owner == id then
        redis.call("DEL", uk)
    end
end

return 1
`)

// nackScript records a failure, increments the attempt counter, and either
// schedules a retry with backoff or sends the job to the dead letter queue.
var nackScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local delayedKey = KEYS[3]
local deadKey = KEYS[4]
local statsKey = KEYS[5]
local workerJobsKey = KEYS[6]
local leaseKey = KEYS[7]

local id = ARGV[1]
local errorMsg = ARGV[2]
local backoffMs = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local skipRetry = tonumber(ARGV[5])

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("ZREM", leaseKey, id)

local attempt = redis.call("HINCRBY", jobKey, "attempt", 1)
redis.call("HSET", jobKey, "last_error", errorMsg)

local maxRetries = tonumber(redis.call("HGET", jobKey, "max_retries"))

if skipRetry == 1 or attempt >= maxRetries then
    redis.call("LPUSH", deadKey, id)
    redis.call("HSET", jobKey, "state", "dead")
    redis.call("HINCRBY", statsKey, "dead", 1)

    local uk = redis.call("HGET", jobKey, "unique_key")
    if uk and uk ~= "" then
        local owner = redis.call("GET", uk)
        if owner == id then
            redis.call("DEL", uk)
        end
    end

    return "dead"
end

local nextRun = now + backoffMs
redis.call("ZADD", delayedKey, nextRun, id)
redis.call("HSET", jobKey, "state", "retry")
return "retry"
`)

// rescheduleScript moves an active job back to the delayed set with a new timestamp.
var rescheduleScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local delayedKey = KEYS[3]
local workerJobsKey = KEYS[4]
local leaseKey = KEYS[5]

local id = ARGV[1]
local newTimestamp = ARGV[2]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("ZREM", leaseKey, id)
redis.call("ZADD", delayedKey, newTimestamp, id)
redis.call("HSET", jobKey, "state", "pending", "scheduled_at", newTimestamp)
return 1
`)

// cancelScript removes a job from active processing and marks it cancelled.
var cancelScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local workerJobsKey = KEYS[3]
local leaseKey = KEYS[4]

local id = ARGV[1]
local now = ARGV[2]
local reason = ARGV[3]

redis.call("SREM", activeKey, id)
redis.call("SREM", workerJobsKey, id)
redis.call("ZREM", leaseKey, id)
redis.call("HSET", jobKey, "state", "cancelled", "completed_at", now, "last_error", reason)
return 1
`)

// extendLeaseScript updates an existing lease expiry using ZADD XX so that only
// jobs currently holding a lease are affected.
var extendLeaseScript = redis.NewScript(`
local leaseKey = KEYS[1]

local id = ARGV[1]
local newExpiry = tonumber(ARGV[2])

local updated = redis.call("ZADD", leaseKey, "XX", newExpiry, id)
return updated
`)

// recoverLeasesScript scans the lease sorted set for expired entries and moves
// them back to ready. Uses SREM on the active set as an idempotent guard so
// concurrent recovery goroutines cannot double-recover the same job.
var recoverLeasesScript = redis.NewScript(`
local leaseKey = KEYS[1]
local activeKey = KEYS[2]
local readyKey = KEYS[3]

local now = ARGV[1]
local limit = tonumber(ARGV[2])

local expired = redis.call("ZRANGEBYSCORE", leaseKey, "-inf", now, "LIMIT", 0, limit)
if #expired == 0 then
    return {}
end

local recovered = {}
for _, id in ipairs(expired) do
    local removed = redis.call("SREM", activeKey, id)
    if removed == 1 then
        redis.call("ZREM", leaseKey, id)
        local jobKey = "winter:job:" .. id
        local priority = tonumber(redis.call("HGET", jobKey, "priority"))
        redis.call("ZADD", readyKey, priority, id)
        redis.call("HSET", jobKey, "state", "pending")
        table.insert(recovered, id)
    else
        redis.call("ZREM", leaseKey, id)
    end
end

return recovered
`)

// promoteScript moves delayed jobs whose scheduled time has passed back into the
// ready set, restoring their original priority.
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

// retryDeadScript removes a job from the dead list, resets its attempt counter
// and state, and re-enqueues it to the ready set at its original priority.
var retryDeadScript = redis.NewScript(`
local jobKey = KEYS[1]
local deadKey = KEYS[2]
local readyKey = KEYS[3]
local statsKey = KEYS[4]

local id = ARGV[1]

local removed = redis.call("LREM", deadKey, 1, id)
if removed == 0 then
    return 0
end

local priority = tonumber(redis.call("HGET", jobKey, "priority"))
redis.call("HSET", jobKey, "state", "pending", "attempt", 0, "last_error", "", "completed_at", 0)
redis.call("ZADD", readyKey, priority, id)
redis.call("HINCRBY", statsKey, "retried", 1)
return 1
`)

// purgeDeadScript removes all jobs from the dead list and deletes their hashes.
var purgeDeadScript = redis.NewScript(`
local deadKey = KEYS[1]

local ids = redis.call("LRANGE", deadKey, 0, -1)
for _, id in ipairs(ids) do
    redis.call("DEL", "winter:job:" .. id)
end
redis.call("DEL", deadKey)
return #ids
`)
