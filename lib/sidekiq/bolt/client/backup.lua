local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local work = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local pool_key = namespace .. 'resource:pool:' .. resource_name
local limit_key = namespace .. 'resource:limit:' .. resource_name

local limit = tonumber(redis.call('get', limit_key))

local pool_allocation = -1

if limit then pool_allocation = redis.call('zrangebyscore', pool_key, '-INF', 'INF', 'LIMIT', 0, 1)[1] end

if pool_allocation then
    redis.call('incr', allocated_key)

    local worker_backup_key = namespace .. 'resource:backup:worker:' .. worker_id
    local queue_busy_key = namespace .. 'queue:busy:' .. queue_name
    local backup_work = { queue = queue_name, allocation = pool_allocation, resource = resource_name, work = work }
    local limit

    redis.call('zrem', pool_key, pool_allocation)
    redis.call('lpush', worker_backup_key, cjson.encode(backup_work))
    redis.call('incr', queue_busy_key)
end

return pool_allocation
