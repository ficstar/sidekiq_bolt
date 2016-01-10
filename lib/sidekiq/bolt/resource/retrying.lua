local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local resource_queues_key = namespace .. 'resource:queues:' .. resource_name
local queue_names = redis.call('smembers', resource_queues_key)

local retry_count = 0

for _, queue in ipairs(queue_names) do
    local retrying_key = namespace .. 'resource:queue:retrying:' .. queue .. ':' .. resource_name
    local resource_retry_count = redis.call('llen', retrying_key)

    retry_count = retry_count + resource_retry_count
end

local retry_set_key = namespace .. 'bolt:retry'
local retry_set = redis.call('zrange', retry_set_key, 0, -1)

for _, retry in ipairs(retry_set) do
    local message = cjson.decode(retry)
    if message.resource == resource_name then
        retry_count = retry_count + 1
    end
end

return retry_count
