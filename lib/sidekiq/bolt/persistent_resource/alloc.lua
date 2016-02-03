local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resources:persistent:backup:worker:' .. worker_id
local resource_key = namespace .. 'resources:persistent:' .. resource_name

local item = redis.call('zrangebyscore', resource_key, '-INF', 'INF', 'LIMIT', 0, 1)[1]
redis.call('zrem', resource_key, item)
redis.call('lpush', worker_backup_key, item)

return item
