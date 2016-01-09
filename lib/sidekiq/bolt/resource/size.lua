local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local queue_prefix = table.remove(ARGV, 1)
local resource_queues_key = namespace .. 'resource:queues:' .. resource_name
local queues = redis.call('smembers', resource_queues_key)
local size = 0

for _, queue in ipairs(queues) do
    local queue_key = namespace .. 'resource:queue:' .. queue_prefix     .. queue .. ':' .. resource_name
    local queue_size = redis.call('llen', queue_key)

    size = size + queue_size
end

return size
