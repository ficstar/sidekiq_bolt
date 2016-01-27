local namespace = table.remove(KEYS, 1)
local parent_job_id = table.remove(ARGV, 1)
local job_id = table.remove(ARGV, 1)
local job_failed = table.remove(ARGV, 1)

local dependencies_key = namespace .. 'dependencies:' .. job_id
redis.call('srem', dependencies_key, job_id)

local resources_key = namespace .. 'resources'
local queues_key = namespace .. 'queues'

local parent_failure_count

while job_id do
    dependencies_key = namespace .. 'dependencies:' .. job_id

    local dependency_count = redis.call('scard', dependencies_key)
    local scheduled_work_key = namespace .. 'successive_work:' .. job_id

    parent_failure_count = 0
    if job_failed then
        local job_failed_key = namespace .. 'job_failed:' .. job_id
        redis.call('set', job_failed_key, 'true')

        if parent_job_id then
            local parent_failure_count_key = namespace .. 'job_failured_count:' .. parent_job_id
            parent_failure_count = redis.call('incr', parent_failure_count_key)
        end
        redis.call('del', scheduled_work_key)
    end

    if dependency_count > 0 then
        job_id = nil
        parent_job_id = nil
    else
        local parent_link_key = namespace .. 'parent:' .. job_id
        local next_parent_link_key
        local next_parent_job_id

        if parent_job_id then
            local parent_dependencies_key = namespace .. 'dependencies:' .. parent_job_id

            redis.call('srem', parent_dependencies_key, job_id)
            redis.call('del', parent_link_key)

            next_parent_link_key = namespace .. 'parent:' .. parent_job_id
            next_parent_job_id = redis.call('get', next_parent_link_key)
        else
            next_parent_job_id = nil
        end

        if job_failed then
            if parent_job_id then
                local parent_failure_limit_key = namespace .. 'job_failure_limit:' .. parent_job_id
                local parent_failure_limit = tonumber(redis.call('get', parent_failure_limit_key))

                if not parent_failure_limit then parent_failure_limit = 0 end

                if parent_failure_count < parent_failure_limit then
                    job_failed = false
                else
                    job_failed = true
                end
            end
        else
            local job_completed_key = namespace .. 'job_completed:' .. job_id
            redis.call('set', job_completed_key, 'true')

            local scheduled_items = redis.call('lrange', scheduled_work_key, 0, -1)

            redis.call('del', scheduled_work_key)
            for _, serialized_work in ipairs(scheduled_items) do
                local work = cjson.decode(serialized_work)

                local queue_blocked_key = namespace .. 'queue:blocked:' .. work.queue
                local queue_blocked = redis.call('get', queue_blocked_key)

                if not queue_blocked then
                    local resource_queues_key = namespace .. 'resource:queues:' .. work.resource
                    local queue_resources_key = namespace .. 'queue:resources:' .. work.queue
                    local queue_key = namespace .. 'resource:queue:' .. work.queue .. ':' .. work.resource
                    redis.call('lpush', queue_key, work.work)

                    redis.call('sadd', resource_queues_key, work.queue)
                    redis.call('sadd', queue_resources_key, work.resource)
                    redis.call('sadd', resources_key, work.resource)
                    redis.call('sadd', queues_key, work.queue)
                end
            end
        end

        job_id = parent_job_id
        parent_job_id = next_parent_job_id
    end
end
