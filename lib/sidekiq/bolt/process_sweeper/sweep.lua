local namespace = table.remove(KEYS, 1)
local process = table.remove(ARGV, 1)
local processes_set_key = namespace .. 'bolt:processes'

local worker_backup_key = namespace .. 'resource:backup:worker:' .. process
local processing_work = redis.call('lrange', worker_backup_key, 0, -1)
for _, serialized_work in ipairs(processing_work) do
    local work = cjson.decode(serialized_work)

    local resource_allocated_key = namespace .. 'resource:allocated:' .. work.resource
    local resource_pool_key = namespace .. 'resource:pool:' .. work.resource
    local queue_key = namespace .. 'resource:queue:retrying:' .. work.queue .. ':' .. work.resource
    local queue_busy_key = namespace .. 'queue:busy:' .. work.queue

    redis.call('lpush', queue_key, work.work)
    redis.call('decr', resource_allocated_key)
    redis.call('decr', queue_busy_key)

    local resource_limit_key = namespace .. 'resource:limit:' .. work.resource
    local resource_limit = redis.call('get', resource_limit_key)

    if resource_limit and tonumber(work.allocation) <= tonumber(resource_limit) then
        redis.call('zadd', resource_pool_key, 0, work.allocation)
    end
end
redis.call('del', worker_backup_key)

local worker_persistent_resource_key = namespace .. 'resources:persistent:backup:worker:' .. process
local persistent_resources = redis.call('lrange', worker_persistent_resource_key, 0, -1)
for _, serialized_item in ipairs(persistent_resources) do
    local item = cjson.decode(serialized_item)
    local resource_key = namespace .. 'resources:persistent:' .. item.resource

    redis.call('zadd', resource_key, 0.0, item.item)
end
redis.call('del', worker_persistent_resource_key)

redis.call('srem', processes_set_key, process)
