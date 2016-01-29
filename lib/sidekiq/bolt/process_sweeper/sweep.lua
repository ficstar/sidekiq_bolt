local namespace = table.remove(KEYS, 1)
local process = table.remove(ARGV, 1)
local processes_set_key = namespace .. 'bolt:processes'

redis.call('srem', processes_set_key, process)
