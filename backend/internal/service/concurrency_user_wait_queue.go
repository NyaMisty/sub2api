package service

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

const (
	userWaitFinalizeTimeout = 5 * time.Second
)

type UserWaitReason string

const (
	UserWaitReasonUserSlot    UserWaitReason = "user_slot"
	UserWaitReasonNoAvailable UserWaitReason = "no_available"
)

type UserWaitState string

const (
	UserWaitStateQueued   UserWaitState = "queued"
	UserWaitStateGranted  UserWaitState = "granted"
	UserWaitStateDone     UserWaitState = "done"
	UserWaitStateCanceled UserWaitState = "canceled"
	UserWaitStateTimedOut UserWaitState = "timed_out"
	UserWaitStateMissing  UserWaitState = "missing"
)

var (
	ErrUserWaitQueueFull        = errors.New("user wait queue full")
	ErrUserWaitQueueUnavailable = errors.New("user wait authority queue unavailable")
)

type UserWaitRequest struct {
	RequestID      string
	UserID         int64
	MaxConcurrency int
	MaxWait        int
	Priority       int
	Reason         UserWaitReason
	GroupID        int64
	Platform       string
	Model          string
	Timeout        time.Duration
}

type UserWaitTicket struct {
	RequestID      string
	UserID         int64
	MaxConcurrency int
	Priority       int
	Reason         UserWaitReason
	GroupID        int64
	Platform       string
	Model          string
	OwnerInstance  string
	Deadline       time.Time
}

type UserWaitPollResult struct {
	State            UserWaitState
	GrantedRequestID string
}

type userWaitAuthorityCache interface {
	TryAcquireUserSlotRespectingQueue(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error)
	EnqueueUserWait(ctx context.Context, ticket *UserWaitTicket, maxWait int) (bool, error)
	PollUserWait(ctx context.Context, requestID string) (*UserWaitPollResult, error)
	CompleteUserWait(ctx context.Context, requestID string, finalState UserWaitState) error
}

func (s *ConcurrencyService) SupportsUserWaitAuthorityQueue() bool {
	if s == nil || s.cache == nil {
		return false
	}
	_, ok := s.cache.(userWaitAuthorityCache)
	return ok
}

func (s *ConcurrencyService) acquireUserSlotInternal(
	ctx context.Context,
	userID int64,
	maxConcurrency int,
	bypassQueue bool,
) (*AcquireResult, error) {
	if s == nil || s.cache == nil {
		return &AcquireResult{
			Acquired:    true,
			ReleaseFunc: func() {},
		}, nil
	}

	requestID := generateRequestID()
	cacheWithQueue, hasQueue := s.cache.(userWaitAuthorityCache)

	if maxConcurrency <= 0 {
		if !bypassQueue && hasQueue {
			acquired, err := cacheWithQueue.TryAcquireUserSlotRespectingQueue(ctx, userID, maxConcurrency, requestID)
			if err != nil {
				return nil, err
			}
			if !acquired {
				return &AcquireResult{Acquired: false}, nil
			}
		}
		return &AcquireResult{
			Acquired:    true,
			ReleaseFunc: func() {},
		}, nil
	}

	var (
		acquired bool
		err      error
	)
	if !bypassQueue && hasQueue {
		acquired, err = cacheWithQueue.TryAcquireUserSlotRespectingQueue(ctx, userID, maxConcurrency, requestID)
	} else {
		acquired, err = s.cache.AcquireUserSlot(ctx, userID, maxConcurrency, requestID)
	}
	if err != nil {
		return nil, err
	}
	if !acquired {
		return &AcquireResult{Acquired: false}, nil
	}

	return &AcquireResult{
		Acquired: true,
		ReleaseFunc: func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.cache.ReleaseUserSlot(bgCtx, userID, requestID); err != nil {
				logger.LegacyPrintf("service.concurrency", "Warning: failed to release user slot for %d (req=%s): %v", userID, requestID, err)
			}
		},
	}, nil
}

func (s *ConcurrencyService) AcquireGrantedUserSlot(ctx context.Context, userID int64, maxConcurrency int) (*AcquireResult, error) {
	return s.acquireUserSlotInternal(ctx, userID, maxConcurrency, true)
}

func (s *ConcurrencyService) EnqueueUserWait(ctx context.Context, req UserWaitRequest) (*UserWaitTicket, error) {
	if s == nil || s.cache == nil {
		return nil, ErrUserWaitQueueUnavailable
	}
	cacheWithQueue, ok := s.cache.(userWaitAuthorityCache)
	if !ok {
		return nil, ErrUserWaitQueueUnavailable
	}
	if req.RequestID == "" {
		req.RequestID = generateRequestID()
	}
	if req.Timeout <= 0 {
		req.Timeout = time.Second
	}
	ticket := &UserWaitTicket{
		RequestID:      req.RequestID,
		UserID:         req.UserID,
		MaxConcurrency: req.MaxConcurrency,
		Priority:       req.Priority,
		Reason:         req.Reason,
		GroupID:        req.GroupID,
		Platform:       req.Platform,
		Model:          req.Model,
		OwnerInstance:  RequestIDPrefix(),
		Deadline:       time.Now().Add(req.Timeout),
	}
	enqueued, err := cacheWithQueue.EnqueueUserWait(ctx, ticket, req.MaxWait)
	if err != nil {
		return nil, err
	}
	if !enqueued {
		return nil, ErrUserWaitQueueFull
	}
	return ticket, nil
}

func (s *ConcurrencyService) PollUserWait(ctx context.Context, requestID string) (*UserWaitPollResult, error) {
	if s == nil || s.cache == nil {
		return nil, ErrUserWaitQueueUnavailable
	}
	cacheWithQueue, ok := s.cache.(userWaitAuthorityCache)
	if !ok {
		return nil, ErrUserWaitQueueUnavailable
	}
	return cacheWithQueue.PollUserWait(ctx, requestID)
}

func (s *ConcurrencyService) CompleteUserWait(ctx context.Context, requestID string, finalState UserWaitState) error {
	if s == nil || s.cache == nil {
		return ErrUserWaitQueueUnavailable
	}
	cacheWithQueue, ok := s.cache.(userWaitAuthorityCache)
	if !ok {
		return ErrUserWaitQueueUnavailable
	}
	return cacheWithQueue.CompleteUserWait(ctx, requestID, finalState)
}

func (s *ConcurrencyService) CompleteUserWaitBestEffort(requestID string, finalState UserWaitState) {
	if s == nil || requestID == "" {
		return
	}
	bgCtx, cancel := context.WithTimeout(context.Background(), userWaitFinalizeTimeout)
	defer cancel()
	if err := s.CompleteUserWait(bgCtx, requestID, finalState); err != nil && !errors.Is(err, ErrUserWaitQueueUnavailable) {
		logger.LegacyPrintf("service.concurrency", "Warning: complete user wait failed for %s (%s): %v", requestID, finalState, err)
	}
}
