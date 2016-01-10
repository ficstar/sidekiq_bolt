local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local queue_resources_key = namespace .. 'queue:resources:' .. queue_name
local resource_names = redis.call('smembers', queue_resources_key)

local retry_count = 0

for _, resource in ipairs(resource_names) do
    local retrying_key = namespace .. 'resource:queue:retrying:' .. queue_name .. ':' .. resource
    local resource_retry_count = redis.call('llen', retrying_key)

    retry_count = retry_count + resource_retry_count
end

local retry_set_key = namespace .. 'bolt:retry'
local retry_set = redis.call('zrange', retry_set_key, 0, -1)

for _, retry in ipairs(retry_set) do
    local message = cjson.decode(retry)
    if message.queue == queue_name then
        retry_count = retry_count + 1
    end
end

return retry_count
