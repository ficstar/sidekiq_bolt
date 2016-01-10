local namespace = table.remove(KEYS, 1)
local retries_key = namespace .. 'bolt:retry'
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local job = table.remove(ARGV, 1)
local retry_in = table.remove(ARGV, 1)
local work = { queue = queue_name, resource = resource_name, work = job }

redis.call('zadd', retries_key, retry_in, cjson.encode(work))
