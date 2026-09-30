#!lua flags=no-writes
--[[
procrastinators: a consistent, read-only look at rules and cooldowns.

Serves advisory inspection and the first half of policy administration. The
caller decodes and validates everything; this only gathers it under one time
sample, so what it reports was true together at that instant.

KEYS: per rule, its policy metadata hash, state hash, and events; then
cooldown hashes. ARGV: time override ('' for server time), rule count.

Replies with the time, then per rule its metadata and state as flat field
lists and its events as members, then each cooldown's end and reason.
]]

local now
if ARGV[1] == '' then
  local time = redis.call('TIME')
  now = tonumber(time[1]) * 1000000 + tonumber(time[2])
else
  now = tonumber(ARGV[1])
end
local count = tonumber(ARGV[2])

local reply = {now}
for index = 1, count do
  local meta = redis.pcall('HGETALL', KEYS[3 * index - 2])
  local state = redis.pcall('HGETALL', KEYS[3 * index - 1])
  local events = redis.pcall('ZRANGE', KEYS[3 * index], 0, -1)
  for _, part in ipairs({meta, state, events}) do
    if type(part) == 'table' and part.err ~= nil then
      return {'corrupt', index, part.err}
    end
  end
  reply[#reply + 1] = meta
  reply[#reply + 1] = state
  reply[#reply + 1] = events
end
for index = 3 * count + 1, #KEYS do
  local cooldown = redis.pcall('HMGET', KEYS[index], 'u', 'r')
  if type(cooldown) == 'table' and cooldown.err ~= nil then
    return {'corrupt', 0, cooldown.err}
  end
  reply[#reply + 1] = {cooldown[1] or '', cooldown[2] or ''}
end
return reply
