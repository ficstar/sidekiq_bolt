local namespace = table.remove(KEYS, 1)
local parent_job_id = table.remove(ARGV, 1)
local job_id = table.remove(ARGV, 1)

local resources_key = namespace .. 'resources'
local queues_key = namespace .. 'queues'

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

        local scheduled_work_key = namespace .. 'successive_work:' .. job_id
        local serialized_work = redis.call('lpop', scheduled_work_key)

        if serialized_work then
            local work = cjson.decode(serialized_work)

            local resource_queues_key = namespace .. 'resource:queues:' .. work.resource
            local queue_resources_key = namespace .. 'queue:resources:' .. work.queue
            local queue_key = namespace .. 'resource:queue:' .. work.queue .. ':' .. work.resource
            redis.call('lpush', queue_key, work.work)

            redis.call('sadd', resource_queues_key, work.queue)
            redis.call('sadd', queue_resources_key, work.resource)
            redis.call('sadd', resources_key, work.resource)
            redis.call('sadd', queues_key, work.queue)
        end

        job_id = parent_job_id
        parent_job_id = next_parent_job_id
    end
end
