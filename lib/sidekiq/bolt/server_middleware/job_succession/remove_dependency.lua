local namespace = table.remove(KEYS, 1)
local parent_job_id = table.remove(ARGV, 1)
local job_id = table.remove(ARGV, 1)
local parent_dependencies_key = namespace .. 'dependencies:' .. parent_job_id
local parent_link_key = namespace .. 'parent:' .. job_id

redis.call('srem', parent_dependencies_key, job_id)
redis.call('del', parent_link_key)
