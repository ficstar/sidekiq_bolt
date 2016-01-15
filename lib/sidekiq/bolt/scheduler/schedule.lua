local namespace = table.remove(KEYS, 1)
local prev_job_id = table.remove(ARGV, 1)
local prev_job_scheduled_key = namespace .. 'successive_work:' .. prev_job_id

while table.getn(ARGV) > 0 do
    local queue_name = table.remove(ARGV, 1)
    local resource_name = table.remove(ARGV, 1)
    local work = table.remove(ARGV, 1)
    local job = { queue = queue_name, resource = resource_name, work = work }

    redis.call('lpush', prev_job_scheduled_key, cjson.encode(job))
end
