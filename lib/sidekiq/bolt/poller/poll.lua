local namespace = table.remove(KEYS, 1)
local set_name = table.remove(ARGV, 1)
local queue_prefix = table.remove(ARGV, 1)
local current_time = table.remove(ARGV, 1)
local set_key = namespace .. set_name
local serialized_retry = redis.call('zrangebyscore', set_key, '-inf', current_time, 'LIMIT', 0, 1)[1]

if serialized_retry then
    local retry = cjson.decode(serialized_retry)
    local resource_queue_key = namespace .. 'resource:queue:' .. queue_prefix .. retry.queue .. ':' .. retry.resource
    local resource_queues_key = namespace .. 'resource:queues:' .. retry.resource
    local queue_resources_key = namespace .. 'queue:resources:' .. retry.queue

    redis.call('sadd', resource_queues_key, retry.queue)
    redis.call('sadd', queue_resources_key, retry.resource)
    redis.call('lpush', resource_queue_key, retry.work)
    redis.call('zrem', set_key, serialized_retry)
end
