-- Atomic check-and-act for one expired in-flight message (design.md §5).
-- KEYS[1] = inflight key
-- KEYS[2] = vis_index key (queue-level)
-- ARGV[1] = receiptHandle
-- ARGV[2] = maxReceiveCount
-- ARGV[3] = queueName
--
-- Returns {"gone"} if another reaper already won the race for this key.
-- Returns {"dlq", topic, shardId, partition, offset, body, groupId, dedupId}
--   when receiveCount has reached maxReceiveCount.
-- Returns {"redeliver", receiptHandle, newReceiveCount} otherwise.

local removed = redis.call('ZREM', KEYS[2], ARGV[1])
if removed == 0 then
  return {'gone'}
end

local inflight = redis.call('HGETALL', KEYS[1])
if #inflight == 0 then
  return {'gone'}
end

local m = {}
for i = 1, #inflight, 2 do
  m[inflight[i]] = inflight[i + 1]
end

local queueName = ARGV[3]
local receiveCount = tonumber(m['receiveCount'] or '0')
local maxReceive = tonumber(ARGV[2])

if m['groupId'] and m['groupId'] ~= '' then
  local lockKey = 'kmsvc:fifo_lock:' .. queueName .. ':' .. m['groupId']
  local lockVal = redis.call('GET', lockKey)
  if lockVal == ARGV[1] then
    redis.call('DEL', lockKey)
  end
end

if receiveCount >= maxReceive then
  local pendingKey = 'kmsvc:pending:' .. queueName .. ':' .. m['shardId'] .. ':' .. m['partition']
  redis.call('ZREM', pendingKey, m['offset'])
  redis.call('DEL', KEYS[1])
  return {'dlq', m['topic'] or '', m['shardId'] or '', m['partition'] or '', m['offset'] or '', m['body'] or '', m['groupId'] or '', m['dedupId'] or ''}
end

redis.call('HSET', KEYS[1], 'receiveCount', receiveCount + 1)
redis.call('RPUSH', 'kmsvc:redeliver:' .. queueName, ARGV[1])
return {'redeliver', ARGV[1], tostring(receiveCount + 1)}
