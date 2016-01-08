local namespace = table.remove(KEYS, 1)
local resources_key = namespace .. 'resources'
local resources = redis.call('smembers', resources_key)
local valid_resources = {}

for _, resource in ipairs(resources) do
    local resource_type_key = namespace .. 'resource:type:' .. resource
    local resource_type = redis.call('get', resource_type_key)
    local resource_supported = false

    if not resource_type then resource_type = 'default' end

    for _, type in ipairs(ARGV) do
        if type == resource_type then resource_supported = true end
    end

    if resource_supported then table.insert(valid_resources, resource) end
end

return valid_resources
