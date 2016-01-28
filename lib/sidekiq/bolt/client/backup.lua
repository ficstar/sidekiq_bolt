local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local work = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local limit_key = namespace .. 'resource:limit:' .. resource_name

local limit = tonumber(redis.call('get', limit_key))
local allocation = redis.call('incr', allocated_key)
local perform_allocation = true

if limit and allocation > limit then
    redis.call('decr', allocated_key)
    perform_allocation = false
end

if perform_allocation then
    local worker_backup_key = namespace .. 'resource:backup:worker:' .. worker_id
    local queue_busy_key = namespace .. 'queue:busy:' .. queue_name
    local backup_work = { queue = queue_name, resource = resource_name, work = work }
    local limit

    redis.call('lpush', worker_backup_key, cjson.encode(backup_work))
    redis.call('incr', queue_busy_key)
end

return perform_allocation
