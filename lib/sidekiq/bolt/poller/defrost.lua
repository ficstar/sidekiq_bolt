local namespace = table.remove(KEYS, 1)
local current_time = table.remove(ARGV, 1)
local set_key = namespace .. 'bolt:frozen_resource'
local frozen_resources = redis.call('zrangebyscore', set_key, '-inf', current_time)

for _, resource_name in ipairs(frozen_resources) do
    local resource_frozen_key = namespace .. 'resource:frozen:' .. resource_name
    redis.call('del', resource_frozen_key)
    redis.call('zrem', set_key, resource_name)
end
