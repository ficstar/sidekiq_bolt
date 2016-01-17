local namespace = table.remove(KEYS, 1)
local queue_name = table.remove(ARGV, 1)
local resource_name = table.remove(ARGV, 1)
local work = table.remove(ARGV, 1)
local worker_id = table.remove(ARGV, 1)
local worker_backup_key = namespace .. 'resource:backup:worker:' .. worker_id
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local queue_busy_key = namespace .. 'queue:busy:' .. queue_name
local backup_work = { queue = queue_name, resource = resource_name, work = work }

local work_removed_count = redis.call('lrem', worker_backup_key, 0, cjson.encode(backup_work))

if work_removed_count > 0 then
    local allocated = redis.call('decr', allocated_key)
    local allocated_diff = 0
    if allocated < 0 then
        local over_allocated_key = namespace .. 'resource:over-allocated:' .. resource_name

        allocated_diff = allocated
        redis.call('decrby', allocated_key, allocated_diff)
        redis.call('decrby', over_allocated_key, allocated_diff)
    end

    redis.call('decrby', queue_busy_key, 1 + allocated_diff)
end
