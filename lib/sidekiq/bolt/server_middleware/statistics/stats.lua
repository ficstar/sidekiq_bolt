local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local queue_name = table.remove(ARGV, 1)
local error = table.remove(ARGV, 1)

local resource_stats_key = namespace .. 'resource:stats:' .. resource_name
local queue_stats_key = namespace .. 'queue:stats:' .. queue_name

if error then
    redis.call('hincrby', resource_stats_key, 'error', 1)
    redis.call('hincrby', queue_stats_key, 'error', 1)
else
    redis.call('hincrby', resource_stats_key, 'successful', 1)
    redis.call('hincrby', queue_stats_key, 'successful', 1)
end
