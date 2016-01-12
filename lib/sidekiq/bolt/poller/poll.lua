local namespace = table.remove(KEYS, 1)
local set_name = table.remove(ARGV, 1)
local queue_prefix = table.remove(ARGV, 1)
local current_time = table.remove(ARGV, 1)
local set_key = namespace .. set_name
local serialized_jobs = redis.call('zrangebyscore', set_key, '-inf', current_time)

for _, serialized_job in ipairs(serialized_jobs) do
    local retry = cjson.decode(serialized_job)
    local resource_queue_key = namespace .. 'resource:queue:' .. queue_prefix .. retry.queue .. ':' .. retry.resource
    local resource_queues_key = namespace .. 'resource:queues:' .. retry.resource
    local queue_resources_key = namespace .. 'queue:resources:' .. retry.queue
    local resources_key = namespace .. 'resources'
    local queues_key = namespace .. 'queues'

    redis.call('sadd', resource_queues_key, retry.queue)
    redis.call('sadd', queue_resources_key, retry.resource)
    redis.call('sadd', queues_key, retry.queue)
    redis.call('sadd', resources_key, retry.resource)
    redis.call('lpush', resource_queue_key, retry.work)
    redis.call('zrem', set_key, serialized_job)
end
