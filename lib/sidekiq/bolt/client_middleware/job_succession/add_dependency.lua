local namespace = table.remove(KEYS, 1)
local parent_job_id = table.remove(ARGV, 1)
local job_id = table.remove(ARGV, 1)
local parent_dependencies_key = namespace .. 'dependencies:' .. parent_job_id
local dependencies_key = namespace .. 'dependencies:' .. job_id
local parent_link_key = namespace .. 'parent:' .. job_id

redis.call('sadd', parent_dependencies_key, job_id)
redis.call('sadd', dependencies_key, job_id)

local running_key = namespace .. 'job_running:' .. job_id
redis.call('set', running_key, 'true')

local prev_parent = redis.call('get', parent_link_key)

if not prev_parent then
    redis.call('set', parent_link_key, parent_job_id)
    prev_parent = parent_job_id
end

return prev_parent == parent_job_id


