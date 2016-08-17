local namespace = table.remove(KEYS, 1)
local resource_name = table.remove(ARGV, 1)
local queues_key = namespace .. 'resource:queues:' .. resource_name
local queue_names = redis.call('smembers', queues_key)
local group = table.remove(ARGV, 1)
local results = {}

for _, queue in ipairs(queue_names) do
    local queue_group_key = namespace .. 'queue:group:' .. queue
    local queue_group = redis.call('get', queue_group_key)

    if queue_group == group then
        table.insert(results, queue)
    end
end

return results
