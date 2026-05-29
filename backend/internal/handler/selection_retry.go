package handler

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
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
	if errors.Is(err, service.ErrNoAvailableCompactAccounts) {
		return false
	}
	if !isOpsNoAvailableAccountError(err) {
		return false
	}
	return !strings.Contains(strings.ToLower(err.Error()), "channel pricing restriction")
}

func (h *GatewayHandler) shouldRetryGatewayNoAvailableSelection(ctx context.Context, groupID *int64, requestedModel string, err error) (bool, error) {
	if h == nil || h.gatewayService == nil || !shouldRetryNoAvailableSelectionError(err) {
		return false, nil
	}
	return h.gatewayService.HasPotentialAccountForRequest(ctx, groupID, requestedModel)
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
	if h == nil || h.gatewayService == nil || !shouldRetryNoAvailableSelectionError(err) {
		return false, nil
	}
	return h.gatewayService.HasPotentialAccountForSelection(
		ctx,
		groupID,
		requestedModel,
		requiredTransport,
		requiredCapability,
		requiredImageCapability,
		requireCompact,
	)
}
