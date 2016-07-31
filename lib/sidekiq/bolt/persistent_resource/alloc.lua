local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resources:persistent:backup:worker:' .. worker_id
local resource_key = namespace .. 'resources:persistent:' .. resource_name

local item = redis.call('zrangebyscore', resource_key, '-INF', 'INF', 'LIMIT', 0, 1, 'WITHSCORES')
if item[1] then
    redis.call('zrem', resource_key, item[1])
end

local backup_item = { resource = resource_name, item = item[1] }
redis.call('lpush', worker_backup_key, cjson.encode(backup_item))

return item
