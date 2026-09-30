#!lua
--[[
procrastinators: write one rule's administrative change, if nothing moved.

Policy administration reads with read.lua, decides in Python with the same
bookkeeping every backend shares, and writes here. The write happens only if
the rule's revision is still the one read, so an admission or another
administrator that got there first sends the caller round again: a
compare-and-swap under the same authority as admission.

KEYS: the rule's policy metadata hash, state hash, and events.
ARGV: layout, expected revision ('' when the hash did not exist), then
'forget' or 'keep', then migration fields as name/value pairs, an empty value
deleting its field.

Replies {'ok'} or {'stale'}.
]]

local revision = redis.call('HGET', KEYS[1], 'rev')
if (revision or '') ~= ARGV[2] then
  return {'stale'}
end
if ARGV[3] == 'forget' then
  redis.call('DEL', KEYS[2], KEYS[3])
  redis.call('HDEL', KEYS[1], 'alg', 'fp', 'sv', 'at')
end
local set = {'lay', ARGV[1]}
local deleted = {}
for index = 4, #ARGV, 2 do
  if ARGV[index + 1] == '' then
    deleted[#deleted + 1] = ARGV[index]
  else
    set[#set + 1] = ARGV[index]
    set[#set + 1] = ARGV[index + 1]
  end
end
redis.call('HSET', KEYS[1], unpack(set))
if #deleted > 0 then
  redis.call('HDEL', KEYS[1], unpack(deleted))
end
redis.call('HINCRBY', KEYS[1], 'rev', 1)
return {'ok'}
