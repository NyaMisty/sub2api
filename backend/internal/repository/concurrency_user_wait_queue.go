package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	userWaitAuthorityQueueKey      = "qos:user_wait:queue"
	userWaitSequenceKey            = "qos:user_wait:seq"
	userWaitRequestKeyPrefix       = "qos:user_wait:req:"
	userWaitGrantKeyPrefix         = "qos:user_wait:grant:"
	userWaitCompletedEntryTTLInSec = 60
)

var (
	enqueueUserWaitScript = redis.NewScript(`
		local queueKey = KEYS[1]
		local seqKey = KEYS[2]
		local reqKey = KEYS[3]
		local waitKey = KEYS[4]

		local requestID = ARGV[1]
		local userID = ARGV[2]
		local maxConcurrency = ARGV[3]
		local priority = tonumber(ARGV[4])
		local reason = ARGV[5]
		local ownerInstance = ARGV[6]
		local createdAtMs = ARGV[7]
		local deadlineMs = ARGV[8]
		local maxWait = tonumber(ARGV[9])
		local waitTTLSeconds = tonumber(ARGV[10])
		local entryTTLSeconds = tonumber(ARGV[11])
		local groupID = ARGV[12]
		local platform = ARGV[13]
		local model = ARGV[14]

		local current = redis.call('GET', waitKey)
		if current == false then
			current = 0
		else
			current = tonumber(current)
		end
		if current >= maxWait then
			return 0
		end

		local seq = redis.call('INCR', seqKey)
		local queueMember = string.format('%020d:%s', seq, requestID)
		redis.call('HSET', reqKey,
			'request_id', requestID,
			'user_id', userID,
			'group_id', groupID,
			'platform', platform,
			'model', model,
			'reason', reason,
			'priority', priority,
			'max_concurrency', maxConcurrency,
			'state', 'queued',
			'owner_instance', ownerInstance,
			'queue_member', queueMember,
			'queue_seq', seq,
			'created_at_unix_ms', createdAtMs,
			'deadline_unix_ms', deadlineMs,
			'updated_at_unix_ms', createdAtMs,
			'wait_count_released', '0',
			'grant_released', '0'
		)
		redis.call('EXPIRE', reqKey, entryTTLSeconds)
		redis.call('ZADD', queueKey, priority, queueMember)
		redis.call('INCR', waitKey)
		redis.call('EXPIRE', waitKey, waitTTLSeconds)
		return 1
	`)

	tryAcquireUserSlotRespectingQueueScript = redis.NewScript(`
		local slotKey = KEYS[1]
		local queueKey = KEYS[2]

		local maxConcurrency = tonumber(ARGV[1])
		local ttl = tonumber(ARGV[2])
		local requestID = ARGV[3]
		local requestPrefix = ARGV[4]
		local waitPrefix = ARGV[5]
		local grantPrefix = ARGV[6]
		local completeTTL = tonumber(ARGV[7])

		local timeResult = redis.call('TIME')
		local nowSec = tonumber(timeResult[1])
		local nowMs = nowSec * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

		local function requestIDFromMember(member)
			local _, _, reqID = string.find(member, '^[^:]+:(.+)$')
			return reqID
		end

		local function decrementIfPositive(key)
			local current = redis.call('GET', key)
			if current ~= false and tonumber(current) > 0 then
				redis.call('DECR', key)
			end
		end

		local function releaseWaitCount(reqKey, userID)
			if redis.call('HGET', reqKey, 'wait_count_released') ~= '1' then
				decrementIfPositive(waitPrefix .. userID)
				redis.call('HSET', reqKey, 'wait_count_released', '1')
			end
		end

		local function releaseGrant(reqKey, userID)
			if redis.call('HGET', reqKey, 'grant_released') ~= '1' then
				decrementIfPositive(grantPrefix .. userID)
				redis.call('HSET', reqKey, 'grant_released', '1')
			end
		end

		local function cleanupQueue()
			local members = redis.call('ZRANGE', queueKey, 0, -1)
			for _, member in ipairs(members) do
				local reqID = requestIDFromMember(member)
				if reqID == nil or reqID == '' then
					redis.call('ZREM', queueKey, member)
				else
					local reqKey = requestPrefix .. reqID
					local state = redis.call('HGET', reqKey, 'state')
					if state == false then
						redis.call('ZREM', queueKey, member)
					else
						local userID = redis.call('HGET', reqKey, 'user_id')
						local deadline = tonumber(redis.call('HGET', reqKey, 'deadline_unix_ms') or '0')
						if deadline > 0 and nowMs >= deadline then
							if state == 'granted' then
								releaseGrant(reqKey, userID)
							end
							releaseWaitCount(reqKey, userID)
							redis.call('HSET', reqKey, 'state', 'timed_out', 'updated_at_unix_ms', nowMs)
							redis.call('ZREM', queueKey, member)
							redis.call('EXPIRE', reqKey, completeTTL)
						elseif state == 'done' or state == 'canceled' or state == 'timed_out' then
							redis.call('ZREM', queueKey, member)
							redis.call('EXPIRE', reqKey, completeTTL)
						else
							return 1
						end
					end
				end
			end
			return 0
		end

		if cleanupQueue() > 0 then
			return 0
		end

		if maxConcurrency ~= nil and maxConcurrency > 0 then
			local expireBefore = nowSec - ttl
			redis.call('ZREMRANGEBYSCORE', slotKey, '-inf', expireBefore)

			local exists = redis.call('ZSCORE', slotKey, requestID)
			if exists ~= false then
				redis.call('ZADD', slotKey, nowSec, requestID)
				redis.call('EXPIRE', slotKey, ttl)
				return 1
			end

			local count = redis.call('ZCARD', slotKey)
			if count < maxConcurrency then
				redis.call('ZADD', slotKey, nowSec, requestID)
				redis.call('EXPIRE', slotKey, ttl)
				return 1
			end
			return 0
		end

		return 1
	`)

	pollUserWaitScript = redis.NewScript(`
		local queueKey = KEYS[1]

		local selfRequestID = ARGV[1]
		local slotTTL = tonumber(ARGV[2])
		local requestPrefix = ARGV[3]
		local waitPrefix = ARGV[4]
		local grantPrefix = ARGV[5]
		local slotPrefix = ARGV[6]
		local grantKeyTTL = tonumber(ARGV[7])
		local completeTTL = tonumber(ARGV[8])

		local timeResult = redis.call('TIME')
		local nowSec = tonumber(timeResult[1])
		local nowMs = nowSec * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

		local function requestIDFromMember(member)
			local _, _, reqID = string.find(member, '^[^:]+:(.+)$')
			return reqID
		end

		local function decrementIfPositive(key)
			local current = redis.call('GET', key)
			if current ~= false and tonumber(current) > 0 then
				redis.call('DECR', key)
			end
		end

		local function releaseWaitCount(reqKey, userID)
			if redis.call('HGET', reqKey, 'wait_count_released') ~= '1' then
				decrementIfPositive(waitPrefix .. userID)
				redis.call('HSET', reqKey, 'wait_count_released', '1')
			end
		end

		local function releaseGrant(reqKey, userID)
			if redis.call('HGET', reqKey, 'grant_released') ~= '1' then
				decrementIfPositive(grantPrefix .. userID)
				redis.call('HSET', reqKey, 'grant_released', '1')
			end
		end

		local function cleanupMember(member)
			local reqID = requestIDFromMember(member)
			if reqID == nil or reqID == '' then
				redis.call('ZREM', queueKey, member)
				return nil, nil, 'missing'
			end

			local reqKey = requestPrefix .. reqID
			local state = redis.call('HGET', reqKey, 'state')
			if state == false then
				redis.call('ZREM', queueKey, member)
				return reqID, reqKey, 'missing'
			end

			local userID = redis.call('HGET', reqKey, 'user_id')
			local deadline = tonumber(redis.call('HGET', reqKey, 'deadline_unix_ms') or '0')
			if deadline > 0 and nowMs >= deadline then
				if state == 'granted' then
					releaseGrant(reqKey, userID)
				end
				releaseWaitCount(reqKey, userID)
				redis.call('HSET', reqKey, 'state', 'timed_out', 'updated_at_unix_ms', nowMs)
				redis.call('ZREM', queueKey, member)
				redis.call('EXPIRE', reqKey, completeTTL)
				return reqID, reqKey, 'timed_out'
			end

			if state == 'done' or state == 'canceled' or state == 'timed_out' then
				redis.call('ZREM', queueKey, member)
				redis.call('EXPIRE', reqKey, completeTTL)
				return reqID, reqKey, state
			end

			return reqID, reqKey, state
		end

		local function eligibleForGrant(reqKey)
			local userID = redis.call('HGET', reqKey, 'user_id')
			local maxConcurrency = tonumber(redis.call('HGET', reqKey, 'max_concurrency') or '0')
			if maxConcurrency <= 0 then
				return true
			end

			local slotKey = slotPrefix .. userID
			local expireBefore = nowSec - slotTTL
			redis.call('ZREMRANGEBYSCORE', slotKey, '-inf', expireBefore)
			local active = tonumber(redis.call('ZCARD', slotKey) or '0')
			local pending = tonumber(redis.call('GET', grantPrefix .. userID) or '0')
			return (active + pending) < maxConcurrency
		end

		local selfState = 'missing'
		local grantedRequestID = ''
		local members = redis.call('ZRANGE', queueKey, 0, -1)
		for _, member in ipairs(members) do
			local reqID, reqKey, state = cleanupMember(member)
			if reqID ~= nil and reqKey ~= nil then
				if state == 'queued' and grantedRequestID == '' and eligibleForGrant(reqKey) then
					local userID = redis.call('HGET', reqKey, 'user_id')
					redis.call('HSET', reqKey,
						'state', 'granted',
						'granted_at_unix_ms', nowMs,
						'updated_at_unix_ms', nowMs
					)
					redis.call('INCR', grantPrefix .. userID)
					redis.call('EXPIRE', grantPrefix .. userID, grantKeyTTL)
					grantedRequestID = reqID
					state = 'granted'
				end
				if reqID == selfRequestID then
					selfState = state
				end
			end
		end

		return {selfState, grantedRequestID}
	`)

	completeUserWaitScript = redis.NewScript(`
		local queueKey = KEYS[1]
		local reqKey = KEYS[2]

		local waitPrefix = ARGV[1]
		local grantPrefix = ARGV[2]
		local finalState = ARGV[3]
		local completeTTL = tonumber(ARGV[4])

		local timeResult = redis.call('TIME')
		local nowMs = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)

		local function decrementIfPositive(key)
			local current = redis.call('GET', key)
			if current ~= false and tonumber(current) > 0 then
				redis.call('DECR', key)
			end
		end

		local state = redis.call('HGET', reqKey, 'state')
		if state == false then
			return 0
		end

		local userID = redis.call('HGET', reqKey, 'user_id')
		if state == 'granted' and redis.call('HGET', reqKey, 'grant_released') ~= '1' then
			decrementIfPositive(grantPrefix .. userID)
			redis.call('HSET', reqKey, 'grant_released', '1')
		end

		if redis.call('HGET', reqKey, 'wait_count_released') ~= '1' then
			decrementIfPositive(waitPrefix .. userID)
			redis.call('HSET', reqKey, 'wait_count_released', '1')
		end

		local queueMember = redis.call('HGET', reqKey, 'queue_member')
		if queueMember ~= false and queueMember ~= '' then
			redis.call('ZREM', queueKey, queueMember)
		end

		redis.call('HSET', reqKey,
			'state', finalState,
			'updated_at_unix_ms', nowMs
		)
		redis.call('EXPIRE', reqKey, completeTTL)
		return 1
	`)
)

func userWaitRequestKey(requestID string) string {
	return fmt.Sprintf("%s%s", userWaitRequestKeyPrefix, requestID)
}

func userWaitEntryTTLSeconds(deadline time.Time, waitTTLSeconds int) int {
	timeoutSeconds := int(time.Until(deadline).Seconds())
	if timeoutSeconds < 1 {
		timeoutSeconds = 1
	}
	entryTTLSeconds := timeoutSeconds + waitTTLSeconds
	if entryTTLSeconds < waitTTLSeconds {
		return waitTTLSeconds
	}
	return entryTTLSeconds
}

func normalizeUserWaitState(raw string) service.UserWaitState {
	switch raw {
	case string(service.UserWaitStateQueued):
		return service.UserWaitStateQueued
	case string(service.UserWaitStateGranted):
		return service.UserWaitStateGranted
	case string(service.UserWaitStateDone):
		return service.UserWaitStateDone
	case string(service.UserWaitStateCanceled):
		return service.UserWaitStateCanceled
	case string(service.UserWaitStateTimedOut):
		return service.UserWaitStateTimedOut
	default:
		return service.UserWaitStateMissing
	}
}

func (c *concurrencyCache) TryAcquireUserSlotRespectingQueue(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error) {
	slotKey := userSlotKey(userID)
	result, err := tryAcquireUserSlotRespectingQueueScript.Run(
		ctx,
		c.rdb,
		[]string{slotKey, userWaitAuthorityQueueKey},
		maxConcurrency,
		c.slotTTLSeconds,
		requestID,
		userWaitRequestKeyPrefix,
		waitQueueKeyPrefix,
		userWaitGrantKeyPrefix,
		userWaitCompletedEntryTTLInSec,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *concurrencyCache) EnqueueUserWait(ctx context.Context, ticket *service.UserWaitTicket, maxWait int) (bool, error) {
	if ticket == nil {
		return false, nil
	}
	createdAt := time.Now().UTC()
	deadline := ticket.Deadline
	if deadline.IsZero() || !deadline.After(createdAt) {
		deadline = createdAt.Add(time.Second)
	}
	result, err := enqueueUserWaitScript.Run(
		ctx,
		c.rdb,
		[]string{
			userWaitAuthorityQueueKey,
			userWaitSequenceKey,
			userWaitRequestKey(ticket.RequestID),
			waitQueueKey(ticket.UserID),
		},
		ticket.RequestID,
		ticket.UserID,
		ticket.MaxConcurrency,
		ticket.Priority,
		string(ticket.Reason),
		ticket.OwnerInstance,
		createdAt.UnixMilli(),
		deadline.UnixMilli(),
		maxWait,
		c.waitQueueTTLSeconds,
		userWaitEntryTTLSeconds(deadline, c.waitQueueTTLSeconds),
		ticket.GroupID,
		ticket.Platform,
		ticket.Model,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *concurrencyCache) PollUserWait(ctx context.Context, requestID string) (*service.UserWaitPollResult, error) {
	values, err := pollUserWaitScript.Run(
		ctx,
		c.rdb,
		[]string{userWaitAuthorityQueueKey},
		requestID,
		c.slotTTLSeconds,
		userWaitRequestKeyPrefix,
		waitQueueKeyPrefix,
		userWaitGrantKeyPrefix,
		userSlotKeyPrefix,
		c.waitQueueTTLSeconds,
		userWaitCompletedEntryTTLInSec,
	).Slice()
	if err != nil {
		return nil, err
	}

	result := &service.UserWaitPollResult{
		State: service.UserWaitStateMissing,
	}
	if len(values) > 0 {
		result.State = normalizeUserWaitState(fmt.Sprint(values[0]))
	}
	if len(values) > 1 {
		result.GrantedRequestID = fmt.Sprint(values[1])
	}
	return result, nil
}

func (c *concurrencyCache) CompleteUserWait(ctx context.Context, requestID string, finalState service.UserWaitState) error {
	_, err := completeUserWaitScript.Run(
		ctx,
		c.rdb,
		[]string{userWaitAuthorityQueueKey, userWaitRequestKey(requestID)},
		waitQueueKeyPrefix,
		userWaitGrantKeyPrefix,
		string(finalState),
		userWaitCompletedEntryTTLInSec,
	).Result()
	return err
}
