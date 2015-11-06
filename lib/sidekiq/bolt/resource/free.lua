local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local queue_busy_key = namespace .. 'queue:busy:' .. queue_name

redis.call('decr', allocated_key)
redis.call('decr', queue_busy_key)
