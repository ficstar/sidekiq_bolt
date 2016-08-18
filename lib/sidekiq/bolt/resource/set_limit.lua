local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local limit_key = namespace .. 'resource:limit:' .. resource_name
local reference_pool_key = namespace .. 'resource:pool:reference:' .. resource_name
local pool_key = namespace .. 'resource:pool:' .. resource_name
local limit = table.remove(ARGV, 1)

local previous_limit = redis.call('get', limit_key)

if not previous_limit then previous_limit = 0 end

if limit then
    redis.call('set', limit_key, limit)

    for allocation = tonumber(previous_limit) + 1, tonumber(limit) do
        redis.call('sadd', reference_pool_key, allocation)
        redis.call('zadd', pool_key, 0, allocation)
    end

    for allocation = tonumber(previous_limit), tonumber(limit) + 1, -1 do
        redis.call('srem', reference_pool_key, allocation)
        redis.call('zrem', pool_key, allocation)
    end
else
    redis.call('del', limit_key)
    redis.call('del', reference_pool_key)
    redis.call('del', pool_key)
end
