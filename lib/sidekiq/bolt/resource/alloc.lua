local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local limit_key = namespace .. 'resource:limit:' .. resource_name
local total_amount = tonumber(table.remove(ARGV, 1))
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resource:backup:worker:' .. worker_id
local frozen_key = namespace .. 'resource:frozen:' .. resource_name
local frozen = redis.call('get', frozen_key)

if frozen then return {} end

local limit = tonumber(redis.call('get', limit_key))
local resource_pool_key = namespace .. 'resource:pool:' .. resource_name
local queue_names = ARGV

local workload = {}

local processes_key = namespace .. 'bolt:processes'
local worker_processes_key = namespace .. 'bolt:processes:' .. worker_id
redis.call('sadd', processes_key, worker_id)
redis.call('expire', worker_processes_key, 60)

for _, queue in ipairs(queue_names) do
    if total_amount <= 0 then break end

    local queue_busy_key = namespace .. 'queue:busy:' .. queue

    local queue_pasued_key = namespace .. 'queue:paused:' .. queue
    local queue_paused = redis.call('get', queue_pasued_key)


    if not queue_paused then
        for _, queue_prefix in ipairs({ 'retrying:', '' }) do

            local amount = total_amount
            local queue_key = namespace .. 'resource:queue:' .. queue_prefix .. queue .. ':' .. resource_name
            local queue_limit = redis.call('llen', queue_key)

            if queue_limit < amount then
                amount = queue_limit
            end

            local allocated = redis.call('incrby', allocated_key, amount)
            if limit then
                local to_return = limit - allocated

                if to_return < 0 then
                    if -to_return > amount then
                        to_return = -amount
                    end
                    amount = amount + to_return

                    if to_return < 0 then
                        redis.call('incrby', allocated_key, to_return)
                    end
                end
            end

            local allocations = redis.call('zrangebyscore', resource_pool_key, '-INF', 'INF', 'LIMIT', 0, amount)
            for _, allocation in ipairs(allocations) do
                redis.call('zrem', resource_pool_key, allocation)
            end

            if amount > 0 then
                local queue_items = redis.call('lrange', queue_key, 0, amount - 1)

                redis.call('ltrim', queue_key, amount, -1)
                redis.call('incrby', queue_busy_key, amount)

                for index, work in ipairs(queue_items) do
                    local allocation = allocations[index]
                    if not allocation then allocation = '-1' end

                    table.insert(workload, queue)
                    table.insert(workload, allocation)
                    table.insert(workload, work)

                    local backup_work = { queue = queue, allocation = allocation, resource = resource_name, work = work }
                    redis.call('lpush', worker_backup_key, cjson.encode(backup_work))
                end

                total_amount = total_amount - amount
            end
        end
    end
end

return workload
