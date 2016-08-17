local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local queue_key = namespace .. 'resource:queue:' .. queue_name .. ':' .. resource_name
local resource_queues_key = namespace .. 'resource:queues:' .. resource_name
local queue_resources_key = namespace .. 'queue:resources:' .. queue_name
local work = table.remove(ARGV, 1)
local retrying = table.remove(ARGV, 1)
local resources_key = namespace .. 'resources'
local queues_key = namespace .. 'queues'

if retrying then
    queue_key = namespace .. 'resource:queue:retrying:' .. queue_name .. ':' .. resource_name
end

redis.call('lpush', queue_key, work)
redis.call('sadd', resource_queues_key, queue_name)
redis.call('sadd', queue_resources_key, resource_name)
redis.call('sadd', resources_key, resource_name)
redis.call('sadd', queues_key, queue_name)
