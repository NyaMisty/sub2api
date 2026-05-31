package handler

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type selectionRetryState struct {
	timeout  time.Duration
	deadline time.Time
	backoff  time.Duration
	started  bool
}

func newSelectionRetryState(timeout time.Duration) *selectionRetryState {
	if timeout <= 0 {
		timeout = maxConcurrencyWait
	}
	return &selectionRetryState{
		timeout: timeout,
		backoff: initialBackoff,
	}
}

func (s *selectionRetryState) Wait(c *gin.Context, helper *ConcurrencyHelper, isStream bool, streamStarted *bool) (bool, error) {
	if s == nil || helper == nil || c == nil {
		return false, nil
	}
	if !s.started {
		s.deadline = time.Now().Add(s.timeout)
		s.started = true
	}

	remaining := time.Until(s.deadline)
	if remaining <= 0 {
		return false, nil
	}

	delay := s.backoff
	if delay <= 0 {
		delay = initialBackoff
	}
	if delay > remaining {
		delay = remaining
	}
	if err := helper.WaitWithPing(c, delay, isStream, streamStarted); err != nil {
		return false, err
	}

	s.backoff = nextBackoff(delay)
	return true, nil
}

func (s *selectionRetryState) WaitForUserSlotReacquire(
	c *gin.Context,
	helper *ConcurrencyHelper,
	userID int64,
	userConcurrency int,
	queuePriority int,
	currentReleaseFunc func(),
	isStream bool,
	streamStarted *bool,
) (func(), bool, error) {
	if s == nil || helper == nil || c == nil {
		return nil, false, nil
	}
	if !s.started {
		s.deadline = time.Now().Add(s.timeout)
		s.started = true
	}

	remaining := time.Until(s.deadline)
	if remaining <= 0 {
		return nil, false, nil
	}

	if !helper.SupportsAuthorityUserQueue() {
		waited, err := s.Wait(c, helper, isStream, streamStarted)
		if err != nil {
			return currentReleaseFunc, false, err
		}
		return currentReleaseFunc, waited, nil
	}

	delay := s.backoff
	if delay <= 0 {
		delay = initialBackoff
	}
	if delay > remaining {
		delay = remaining
	}

	releaseFunc, acquired, err := helper.WaitForNoAvailableUserSlotReacquire(
		c,
		userID,
		userConcurrency,
		queuePriority,
		remaining,
		delay,
		currentReleaseFunc,
		isStream,
		streamStarted,
	)
	if acquired {
		s.backoff = nextBackoff(delay)
		return releaseFunc, true, nil
	}
	return nil, false, err
}

func (s *selectionRetryState) WaitForRetryableFailoverReacquire(
	c *gin.Context,
	helper *ConcurrencyHelper,
	userID int64,
	userConcurrency int,
	queuePriority int,
	currentReleaseFunc func(),
	isStream bool,
	streamStarted *bool,
	failoverErr *service.UpstreamFailoverError,
) (func(), bool, error) {
	if s == nil {
		return currentReleaseFunc, false, nil
	}
	if !shouldRetryWaitQueueAfterFailover(failoverErr) {
		return currentReleaseFunc, false, nil
	}
	if c != nil && c.Request != nil {
		logger.FromContext(c.Request.Context()).Info("gateway.failover_wait_queue_retry",
			zap.Int("upstream_status", failoverErr.StatusCode),
			zap.Duration("timeout", s.timeout),
		)
	}
	return s.WaitForUserSlotReacquire(
		c,
		helper,
		userID,
		userConcurrency,
		queuePriority,
		currentReleaseFunc,
		isStream,
		streamStarted,
	)
}

func selectionRetryTimeoutFromConfig(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.Gateway.Scheduling.FallbackWaitTimeout > 0 {
		return cfg.Gateway.Scheduling.FallbackWaitTimeout
	}
	return 30 * time.Second
}

func shouldRetryNoAvailableSelectionError(err error) bool {
	if err == nil {
		return true
	}
	if !isOpsNoAvailableAccountError(err) {
		return false
	}
	return !strings.Contains(strings.ToLower(err.Error()), "channel pricing restriction")
}

func shouldRetryWaitQueueAfterFailover(failoverErr *service.UpstreamFailoverError) bool {
	if failoverErr == nil {
		return false
	}
	return failoverErr.StatusCode == http.StatusUnauthorized
}

func (h *GatewayHandler) shouldRetryGatewayNoAvailableSelection(ctx context.Context, groupID *int64, requestedModel string, err error) (bool, error) {
	_ = h
	_ = ctx
	_ = groupID
	_ = requestedModel
	return shouldRetryNoAvailableSelectionError(err), nil
}

func (h *OpenAIGatewayHandler) shouldRetryOpenAINoAvailableSelection(
	ctx context.Context,
	groupID *int64,
	requestedModel string,
	requiredTransport service.OpenAIUpstreamTransport,
	requiredCapability service.OpenAIEndpointCapability,
	requiredImageCapability service.OpenAIImagesCapability,
	requireCompact bool,
	err error,
) (bool, error) {
	_ = h
	_ = ctx
	_ = groupID
	_ = requestedModel
	_ = requiredTransport
	_ = requiredCapability
	_ = requiredImageCapability
	_ = requireCompact
	return shouldRetryNoAvailableSelectionError(err), nil
}
