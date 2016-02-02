local namespace = table.remove(KEYS, 1)
local process = table.remove(ARGV, 1)
local processes_set_key = namespace .. 'bolt:processes'
local worker_backup_key = namespace .. 'resource:backup:worker:' .. process

local processing_work = redis.call('lrange', worker_backup_key, 0, -1)

for _, serialized_work in ipairs(processing_work) do
    local work = cjson.decode(serialized_work)

    local resource_allocated_key = namespace .. 'resource:allocated:' .. work.resource
    local queue_key = namespace .. 'resource:queue:retrying:' .. work.queue .. ':' .. work.resource
    local queue_busy_key = namespace .. 'queue:busy:' .. work.queue

    redis.call('lpush', queue_key, work.work)
    redis.call('decr', resource_allocated_key)
    redis.call('decr', queue_busy_key)
end
redis.call('del', worker_backup_key)

redis.call('srem', processes_set_key, process)