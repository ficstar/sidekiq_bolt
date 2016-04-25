local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local resource = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resources:persistent:backup:worker:' .. worker_id
local resource_key = namespace .. 'resources:persistent:' .. resource_name

redis.call('zrem', resource_key, resource)

local backup_item = { resource = resource_name, item = resource }
redis.call('lrem', worker_backup_key, 0, cjson.encode(backup_item))

