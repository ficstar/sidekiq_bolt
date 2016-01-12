local namespace = table.remove(KEYS, 1)
local retries_key = namespace .. 'bolt:scheduled'
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local job = table.remove(ARGV, 1)
local run_at = table.remove(ARGV, 1)
local work = { queue = queue_name, resource = resource_name, work = job }

redis.call('zadd', retries_key, run_at, cjson.encode(work))
