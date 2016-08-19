local namespace = table.remove(KEYS, 1)
local name = table.remove(ARGV, 1)
local limit_key = namespace .. 'resource:limit:' .. name
local pool_key = namespace .. 'resource:pool:' .. name

local limit = redis.call('get', limit_key)

if not limit then return -1 end

local pool_size = redis.call('zcard', pool_key)

return tonumber(pool_size)