#!lua
--[[
procrastinators: one atomic admission of every rule of a request.

Native executors for the five built-in algorithms, written against the same
contract as the reference evaluators in procrastinators.algorithms and
verified against the same traces. Numbers here are IEEE doubles, exact for
integers up to 2^53 - 1 (contract N3); every product that could exceed that
is taken apart first.

The script runs in three phases, and only the last one writes:

1. Load and check. Sample time, read every rule's policy metadata, migration
   record, and state, and refuse on a policy conflict, a foreign layout, or
   malformed state, returning a reply rather than raising.
2. Evaluate. Every rule against the one time sample, cooldown holds applied.
3. Write. Policy metadata on first contact, then every debit if every rule
   admitted, otherwise only initial state and pruning.

Redis does not roll a script back when it fails part-way, so everything that
can fail on the data is found in phases 1 and 2. A script error during phase 3
is reported by the caller as an indeterminate admission, never as permission.

KEYS, per rule i: 3i-2 policy metadata hash, 3i-1 state hash, 3i events.
Then any cooldown hashes the rules refer to.

ARGV: layout, time override ('' for server time; with an override nothing
expires, since the server's clock is not the authority), cost, rule count, then per
rule: algorithm, fingerprint, state version, four parameters, cooldown
reference ('' none, 'k<j>' KEYS[j], 'v<until>' a value read beforehand).
]]

local LAYOUT = ARGV[1]
local MAX_TIMESTAMP = 9007199254740991
local MAX_COST = 2147483647
local FIELDS = 8
local FIRST = 5
local BATCH = 256

local function fmt(number)
  return string.format('%.0f', number)
end

-- Exact floor division of integers below 2^53 in magnitude.
local function idiv(numerator, denominator)
  local quotient = math.floor(numerator / denominator)
  local remainder = numerator - quotient * denominator
  if remainder < 0 then
    quotient = quotient - 1
  elseif remainder >= denominator then
    quotient = quotient + 1
  end
  return quotient
end

local function ceil_div(numerator, denominator)
  return -idiv(-numerator, denominator)
end

-- floor(count * period / amount) and whether it divides exactly, for
-- count < 2^31, amount < 2^31, period < 2^53, without forming the product.
local function mul_div(count, period, amount)
  local whole = idiv(period, amount)
  local part = period - whole * amount
  local high = math.floor(count / 65536)
  local low = count - high * 65536
  local upper = high * part
  local upper_quotient = idiv(upper, amount)
  local lower = (upper - upper_quotient * amount) * 65536 + low * part
  local lower_quotient = idiv(lower, amount)
  local floor = count * whole + upper_quotient * 65536 + lower_quotient
  return floor, lower - lower_quotient * amount == 0
end

local function integer(text, low, high)
  if type(text) ~= 'string' or not string.match(text, '^%-?%d+$') then
    return nil
  end
  local number = tonumber(text)
  if number == nil or number < low or number > high then
    return nil
  end
  return number
end

local function failed(reply)
  return type(reply) == 'table' and reply.err ~= nil
end

local function as_map(flat)
  local map = {}
  for index = 1, #flat, 2 do
    map[flat[index]] = flat[index + 1]
  end
  return map
end

local function event_cost(member)
  local at, cost = string.match(member, '^(%d+):(%d+):%d+$')
  if at == nil then
    return nil, nil
  end
  return integer(at, 0, MAX_TIMESTAMP), integer(cost, 1, MAX_COST)
end

-- Phase 1: load and check.

-- Expiry runs on the server's clock, so it follows the safe-forget horizon only
-- when that clock is the authority; under an injected clock state persists.
local server_time = ARGV[2] == ''
local now
local server_now
if server_time then
  local time = redis.call('TIME')
  now = tonumber(time[1]) * 1000000 + tonumber(time[2])
  server_now = now
else
  now = tonumber(ARGV[2])
end
local cost = tonumber(ARGV[3])
local count = tonumber(ARGV[4])

local NATIVE = {
  fixed_window = '1', sliding_log = '1', sliding_counter = '1',
  token_bucket = '1', leaky_bucket = '1',
}

local rules = {}
for index = 1, count do
  local base = FIRST + (index - 1) * FIELDS
  local rule = {
    index = index,
    algorithm = ARGV[base],
    fingerprint = ARGV[base + 1],
    state_version = ARGV[base + 2],
    p1 = tonumber(ARGV[base + 3]),
    p2 = tonumber(ARGV[base + 4]),
    p3 = tonumber(ARGV[base + 5]),
    p4 = tonumber(ARGV[base + 6]),
    cooldown = ARGV[base + 7],
    meta_key = KEYS[3 * index - 2],
    state_key = KEYS[3 * index - 1],
    events_key = KEYS[3 * index],
  }
  if NATIVE[rule.algorithm] ~= rule.state_version then
    return {'unsupported', index, rule.algorithm .. ' state ' .. rule.state_version}
  end

  local meta = redis.pcall('HGETALL', rule.meta_key)
  if failed(meta) then
    return {'corrupt', index, 'policy metadata is not a hash: ' .. meta.err}
  end
  meta = as_map(meta)
  if next(meta) ~= nil and meta.lay ~= LAYOUT then
    return {'layout', index, meta.lay or 'none'}
  end
  local last = 0
  if meta.last ~= nil then
    last = integer(meta.last, 0, MAX_TIMESTAMP)
    if last == nil then
      return {'corrupt', index, 'last observed time is malformed'}
    end
  end
  if last > now then
    now = last
  end
  rule.meta = meta

  local status = meta.ms
  if status == 'draining' or status == 'ready' then
    return {'conflict', index, 'migrating', meta.mf or ''}
  elseif meta.fp ~= nil then
    if meta.fp ~= rule.fingerprint then
      return {'conflict', index, 'stored', meta.fp}
    end
  elseif status == 'complete' and meta.mt ~= nil and meta.mv ~= nil then
    if meta.mt ~= rule.fingerprint or meta.mv ~= rule.state_version then
      return {'conflict', index, 'migrated', meta.mt}
    end
  end

  local state = redis.pcall('HGETALL', rule.state_key)
  if failed(state) then
    return {'corrupt', index, 'state is not a hash: ' .. state.err}
  end
  state = as_map(state)
  rule.exists = next(state) ~= nil
  rule.scalars = {}
  rule.n, rule.total, rule.sequence = 0, 0, 0
  if rule.exists then
    if state.x ~= '1' then
      return {'corrupt', index, 'state carries no existence marker'}
    end
    for field, value in pairs(state) do
      if string.sub(field, 1, 2) == 'v:' then
        local number = integer(value, -MAX_TIMESTAMP, MAX_TIMESTAMP)
        if number == nil then
          return {'corrupt', index, 'scalar ' .. field .. ' is malformed'}
        end
        rule.scalars[string.sub(field, 3)] = number
      end
    end
    if rule.algorithm == 'sliding_log' then
      rule.n = integer(state.n or '0', 0, MAX_COST)
      rule.total = integer(state.t or '0', 0, MAX_TIMESTAMP)
      rule.sequence = integer(state.q or '0', 0, MAX_TIMESTAMP)
      if rule.n == nil or rule.total == nil or rule.sequence == nil then
        return {'corrupt', index, 'event log counters are malformed'}
      end
    end
  end
  if rule.algorithm == 'sliding_log' then
    local stored = redis.pcall('ZCARD', rule.events_key)
    if failed(stored) or stored ~= rule.n then
      return {'corrupt', index, 'event log does not match its counters'}
    end
  end
  rules[index] = rule
end

local function scalar(rule, name)
  return rule.scalars[name] or 0
end

-- Phase 2: evaluate. Each returns admitted, retry, horizon (false: never),
-- remaining (false: none); and fills rule.set (scalars on admission),
-- rule.initial (scalars written admitted or not), rule.prune, rule.append.

local function fixed_window(rule)
  local amount, period, offset = rule.p1, rule.p2, rule.p3
  local start = idiv(now - offset, period) * period + offset
  local finish = start + period
  local used = 0
  if rule.exists and scalar(rule, 'window') == start then
    used = scalar(rule, 'count')
  end
  if used + cost <= amount then
    rule.set = {window = start, count = used + cost}
    return true, 0, finish, amount - used - cost
  end
  return false, finish - now, finish, amount - used
end

local function sliding_log(rule)
  local amount, period = rule.p1, rule.p2
  local cutoff = now - period
  local pruned_n, pruned_total = 0, 0
  if rule.n > 0 then
    local old = redis.call('ZRANGEBYSCORE', rule.events_key, '-inf', fmt(cutoff))
    for _, member in ipairs(old) do
      local _, weight = event_cost(member)
      if weight == nil then
        return nil, 'event ' .. member .. ' is malformed'
      end
      pruned_n = pruned_n + 1
      pruned_total = pruned_total + weight
    end
  end
  rule.prune = cutoff
  rule.pruned_n, rule.pruned_total = pruned_n, pruned_total
  local live_n = rule.n - pruned_n
  local live = rule.total - pruned_total
  local newest = now
  if live_n > 0 then
    local top = redis.call('ZREVRANGE', rule.events_key, 0, 0)
    newest = event_cost(top[1])
    if newest == nil then
      return nil, 'event ' .. top[1] .. ' is malformed'
    end
  end
  local forget = newest + period
  if live_n > amount then
    return false, period, forget, 0
  elseif live + cost <= amount then
    rule.append = true
    return true, 0, now + period, amount - live - cost
  end
  local excess = live + cost - amount
  local freed = 0
  local offset = 0
  while true do
    local batch = redis.call(
      'ZRANGEBYSCORE', rule.events_key, '(' .. fmt(cutoff), '+inf', 'LIMIT', offset, BATCH
    )
    if #batch == 0 then
      return nil, 'event log holds less than its counters claim'
    end
    for _, member in ipairs(batch) do
      local at, weight = event_cost(member)
      if weight == nil then
        return nil, 'event ' .. member .. ' is malformed'
      end
      freed = freed + weight
      if freed >= excess then
        return false, at + period - now, forget, math.max(0, amount - live)
      end
    end
    offset = offset + BATCH
  end
end

local function token_bucket(rule)
  local capacity, refill, refill_period, starting = rule.p1, rule.p2, rule.p3, rule.p4
  local tokens, anchor
  if rule.exists then
    tokens, anchor = scalar(rule, 'tokens'), scalar(rule, 'anchor')
  else
    tokens, anchor = starting, now
    rule.initial = {tokens = starting, anchor = now}
  end
  local refills = idiv(math.max(0, now - anchor), refill_period)
  if refills >= ceil_div(capacity - tokens, refill) then
    tokens, anchor = capacity, math.max(anchor, now)
  else
    tokens, anchor = tokens + refills * refill, anchor + refills * refill_period
  end
  local function full_at(balance)
    if starting < capacity then
      return false
    end
    return anchor + ceil_div(capacity - balance, refill) * refill_period
  end
  if cost <= tokens then
    rule.set = {tokens = tokens - cost, anchor = anchor}
    return true, 0, full_at(tokens - cost), tokens - cost
  end
  local periods = ceil_div(cost - tokens, refill)
  return false, anchor + periods * refill_period - now, full_at(tokens), tokens
end

local function leaky_bucket(rule)
  local amount, period, burst = rule.p1, rule.p2, rule.p3
  local tat = now
  if rule.exists then
    tat = scalar(rule, 'tat')
  end
  local tolerance = mul_div(burst, period, amount)
  if tat - now <= tolerance then
    local interval, exact = mul_div(cost, period, amount)
    if not exact then
      interval = interval + 1
    end
    local scheduled = math.max(tat, now) + interval
    if scheduled > MAX_TIMESTAMP then
      return nil, 'invalid'
    end
    rule.set = {tat = scheduled}
    return true, 0, scheduled, false
  end
  return false, tat - tolerance - now, tat, false
end

local function earliest(amount, period, previous, current, elapsed)
  local slack = amount - current - cost
  if slack < 0 then
    return nil
  elseif previous == 0 then
    return elapsed
  end
  local bound = math.max(elapsed, period - idiv((slack + 1) * period - 1, previous))
  if bound < period then
    return bound
  end
  return nil
end

local function sliding_counter(rule)
  local amount, period = rule.p1, rule.p2
  local start = idiv(now, period) * period
  local stored = scalar(rule, 'window')
  local previous, current = 0, 0
  if rule.exists and stored == start then
    previous, current = scalar(rule, 'previous'), scalar(rule, 'current')
  elseif rule.exists and stored == start - period then
    previous = scalar(rule, 'current')
  end
  local elapsed = now - start
  local weighted = idiv(previous * (period - elapsed), period)
  if weighted + current + cost <= amount then
    rule.set = {window = start, previous = previous, current = current + cost}
    return true, 0, start + 2 * period, amount - weighted - current - cost
  end
  local delay
  local within = earliest(amount, period, previous, current, elapsed)
  if within ~= nil then
    delay = start + within - now
  else
    local following = earliest(amount, period, current, 0, 0)
    if following ~= nil then
      delay = start + period + following - now
    else
      delay = start + 2 * period - now
    end
  end
  return false, delay, stored + 2 * period, math.max(0, amount - weighted - current)
end

local EVALUATORS = {
  fixed_window = fixed_window, sliding_log = sliding_log, token_bucket = token_bucket,
  leaky_bucket = leaky_bucket, sliding_counter = sliding_counter,
}

local all_admitted = true
local retry = 0
for index = 1, count do
  local rule = rules[index]
  local admitted, delay, horizon, remaining = EVALUATORS[rule.algorithm](rule)
  if admitted == nil then
    if delay == 'invalid' then
      return {'invalid', index, fmt(now)}
    end
    return {'corrupt', index, delay}
  end
  local reference = rule.cooldown
  local hold_until = nil
  if string.sub(reference, 1, 1) == 'k' then
    local until_text = redis.pcall('HGET', KEYS[tonumber(string.sub(reference, 2))], 'u')
    if failed(until_text) then
      return {'corrupt', index, 'cooldown is not a hash: ' .. until_text.err}
    elseif until_text then
      hold_until = integer(until_text, 0, MAX_TIMESTAMP)
      if hold_until == nil then
        return {'corrupt', index, 'cooldown end is malformed'}
      end
    end
  elseif string.sub(reference, 1, 1) == 'v' then
    hold_until = tonumber(string.sub(reference, 2))
  end
  if hold_until ~= nil and hold_until > now then
    admitted = false
    delay = math.max(hold_until - now, delay)
  end
  rule.admitted, rule.horizon, rule.remaining = admitted, horizon, remaining
  if not admitted then
    all_admitted = false
    retry = math.max(retry, delay)
  end
end

-- Phase 3: write.

local function write_state(rule, scalars)
  local fields = {'x', '1', 'h', rule.horizon and fmt(rule.horizon) or '-'}
  for name, value in pairs(scalars) do
    fields[#fields + 1] = 'v:' .. name
    fields[#fields + 1] = fmt(value)
  end
  if rule.algorithm == 'sliding_log' then
    fields[#fields + 1] = 'n'
    fields[#fields + 1] = fmt(rule.n)
    fields[#fields + 1] = 't'
    fields[#fields + 1] = fmt(rule.total)
    fields[#fields + 1] = 'q'
    fields[#fields + 1] = fmt(rule.sequence)
  end
  redis.call('HSET', rule.state_key, unpack(fields))
  local keys = {rule.state_key}
  if rule.n > 0 then
    keys[2] = rule.events_key
  elseif rule.algorithm == 'sliding_log' then
    redis.call('DEL', rule.events_key)
  end
  for _, key in ipairs(keys) do
    if rule.horizon and server_time then
      -- Counted from the server's own clock, which expiry runs on: when authority
      -- time was clamped above it, the state must outlive the clamp as well.
      redis.call('PEXPIRE', key, fmt(math.max(1, ceil_div(rule.horizon - server_now, 1000))))
    else
      redis.call('PERSIST', key)
    end
  end
end

for index = 1, count do
  local rule = rules[index]
  local meta = {'lay', LAYOUT}
  if rule.meta.fp == nil then
    meta[#meta + 1] = 'alg'
    meta[#meta + 1] = rule.algorithm
    meta[#meta + 1] = 'fp'
    meta[#meta + 1] = rule.fingerprint
    meta[#meta + 1] = 'sv'
    meta[#meta + 1] = rule.state_version
    meta[#meta + 1] = 'at'
    meta[#meta + 1] = fmt(now)
  end
  meta[#meta + 1] = 'last'
  meta[#meta + 1] = fmt(now)
  -- Every write moves the revision, so an administrator's compare-and-swap
  -- that read the rule before this admission goes round again.
  meta[#meta + 1] = 'rev'
  meta[#meta + 1] = fmt((tonumber(rule.meta.rev) or 0) + 1)
  redis.call('HSET', rule.meta_key, unpack(meta))

  local scalars = rule.initial or {}
  local changed = rule.initial ~= nil
  if all_admitted and rule.set then
    for name, value in pairs(rule.set) do
      scalars[name] = value
    end
    changed = true
  end
  if rule.prune and rule.pruned_n > 0 then
    redis.call('ZREMRANGEBYSCORE', rule.events_key, '-inf', fmt(rule.prune))
    rule.n = rule.n - rule.pruned_n
    rule.total = rule.total - rule.pruned_total
    changed = true
  end
  if all_admitted and rule.append then
    rule.sequence = rule.sequence + 1
    local member = fmt(now) .. ':' .. fmt(cost) .. ':' .. fmt(rule.sequence)
    redis.call('ZADD', rule.events_key, fmt(now), member)
    rule.n = rule.n + 1
    rule.total = rule.total + cost
    changed = true
  end
  if changed then
    write_state(rule, scalars)
  end
end

local reply = {'ok', all_admitted and 1 or 0, now, retry}
for index = 1, count do
  local rule = rules[index]
  reply[#reply + 1] = rule.admitted and 1 or 0
  reply[#reply + 1] = rule.remaining and fmt(rule.remaining) or ''
end
return reply
