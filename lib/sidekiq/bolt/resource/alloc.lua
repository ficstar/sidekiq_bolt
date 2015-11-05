local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local limit_key = namespace .. 'resource:limit:' .. resource_name
local amount = tonumber(table.remove(ARGV, 1))

local limit = tonumber(redis.call('get', limit_key))

local allocated = redis.call('incrby', allocated_key, amount)
if limit then
    local to_return = limit - allocated

    if to_return < 0 then
        redis.call('incrby', allocated_key, to_return)
    end
end
