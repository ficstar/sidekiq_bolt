local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local allocated_key = namespace .. 'resource:allocated:' .. resource_name
local limit_key = namespace .. 'resource:limit:' .. resource_name
local amount = tonumber(table.remove(ARGV, 1))

local limit = tonumber(redis.call('get', limit_key))

local queue_key = namespace .. 'resource:queue:' .. 'queue' .. ':' .. 'resourceful'
local queue_busy_key = namespace .. 'queue:busy:' .. 'queue'
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

        redis.call('incrby', allocated_key, to_return)
    end
end

local queue_items = redis.call('lrange', queue_key, 0, amount - 1)
local workload = {}

redis.call('incrby', queue_busy_key, table.getn(queue_items))

for _, work in ipairs(queue_items) do
    table.insert(workload, 'queue')
    table.insert(workload, work)
end

return workload
