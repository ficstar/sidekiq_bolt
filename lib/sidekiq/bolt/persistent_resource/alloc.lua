local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local resource_key = namespace .. 'resources:persistent:' .. resource_name

local item = redis.call('zrangebyscore', resource_key, '-INF', 'INF', 'LIMIT', 0, 1)[1]

return item
