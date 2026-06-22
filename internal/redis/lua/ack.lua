-- Atomic ack for DeleteMessage (design.md §3).
-- KEYS[1] = inflight key
-- KEYS[2] = vis_index key (queue-level)
-- ARGV[1] = receiptHandle
-- ARGV[2] = queueName
--
-- Returns {"not_found"} if the message is already gone (already acked, or
-- already DLQ-routed/redelivered by the reaper).
-- Returns {"acked"} otherwise.

local inflight = redis.call('HGETALL', KEYS[1])
if #inflight == 0 then
  return {'not_found'}
end

local m = {}
for i = 1, #inflight, 2 do
  m[inflight[i]] = inflight[i + 1]
end

local queueName = ARGV[2]

redis.call('ZREM', KEYS[2], ARGV[1])

local pendingKey = 'kmsvc:pending:' .. queueName .. ':' .. m['shardId'] .. ':' .. m['partition']
redis.call('ZREM', pendingKey, m['offset'])
redis.call('DEL', KEYS[1])

if m['groupId'] and m['groupId'] ~= '' then
  local lockKey = 'kmsvc:fifo_lock:' .. queueName .. ':' .. m['groupId']
  local lockVal = redis.call('GET', lockKey)
  if lockVal == ARGV[1] then
    redis.call('DEL', lockKey)
  end
end

return {'acked'}
