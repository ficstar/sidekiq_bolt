local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local work = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resource:backup:worker:' .. worker_id
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local queue_busy_key = namespace .. 'queue:busy:' .. queue_name
local backup_work = { queue = queue_name, resource = resource_name, work = work }

redis.call('lpush', worker_backup_key, cjson.encode(backup_work))
redis.call('incr', allocated_key)
redis.call('incr', queue_busy_key)