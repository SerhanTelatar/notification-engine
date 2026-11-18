-- GCRA slot reservation with zero burst (research R8, ADR 0004).
--
-- KEYS[1]  ratelimit:{channel}: the theoretical arrival time (TAT) of the next free slot, in µs
-- ARGV[1]  emission interval T in µs (1s / limit)
-- ARGV[2]  the longest the caller is willing to wait, in µs
--
-- Returns the µs to wait before using the reserved slot (0 = now), or -1 if the next free slot is
-- further away than the caller will wait; nothing is reserved in that case.
--
-- Slots are handed out exactly T apart across every caller, so any window of one second contains
-- at most 1s/T permits, however many workers share the key. Redis TIME is the only clock, so
-- worker clock skew can't break the limit.

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local max_wait = tonumber(ARGV[2])

local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
if tat < now then
  tat = now
end

local wait = tat - now
if wait > max_wait then
  return -1
end

local next_tat = tat + interval
-- Expire the key once it no longer matters (all reserved slots are in the past).
local ttl_ms = math.floor((next_tat - now) / 1000) + 1000
redis.call('SET', KEYS[1], string.format('%d', next_tat), 'PX', ttl_ms)
return wait
