#!lua
--[[
procrastinators: extend a scope's cooldown to at least now + duration (K2).

A cooldown only ever lengthens: one already lasting as long is left as it is.
The hash expires once the cooldown has passed, which is when it stops
mattering; under an injected clock, which the server's expiry cannot follow,
it persists.

KEYS[1]: the cooldown hash. ARGV: time override ('' for server time),
duration in microseconds, reason, and the largest supported timestamp.

Replies with the end and reason now in force, or {'range'} when the cooldown
would end beyond the supported timestamps, having changed nothing.
]]

local server_time = ARGV[1] == ''
local now
if server_time then
  local time = redis.call('TIME')
  now = tonumber(time[1]) * 1000000 + tonumber(time[2])
else
  now = tonumber(ARGV[1])
end
local finish = now + tonumber(ARGV[2])
if finish > tonumber(ARGV[4]) then
  return {'range'}
end
local existing = redis.call('HMGET', KEYS[1], 'u', 'r')
if existing[1] and string.match(existing[1], '^%d+$') and tonumber(existing[1]) >= finish then
  return {existing[1], existing[2] or ''}
end
local until_text = string.format('%.0f', finish)
redis.call('HSET', KEYS[1], 'u', until_text, 'r', ARGV[3])
if server_time then
  -- One millisecond over, so rounding never lets the hash expire early.
  local ttl = math.max(1, math.ceil((finish - now) / 1000) + 1)
  redis.call('PEXPIRE', KEYS[1], string.format('%.0f', ttl))
end
return {until_text, ARGV[3]}
