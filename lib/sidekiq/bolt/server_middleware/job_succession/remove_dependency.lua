local namespace = table.remove(KEYS, 1)
local parent_job_id = table.remove(ARGV, 1)
local job_id = table.remove(ARGV, 1)

while parent_job_id do
    local dependencies_key = namespace .. 'dependencies:' .. job_id
    local parent_link_key = namespace .. 'parent:' .. job_id
    local dependency_count = redis.call('scard', dependencies_key)

    if dependency_count > 0 then
        parent_job_id = nil
    else
        local parent_dependencies_key = namespace .. 'dependencies:' .. parent_job_id

        redis.call('srem', parent_dependencies_key, job_id)
        redis.call('del', parent_link_key)

        local next_parent_link_key = namespace .. 'parent:' .. parent_job_id
        local next_parent_job_id = redis.call('get', next_parent_link_key)

        job_id = parent_job_id
        parent_job_id = next_parent_job_id
    end
end
