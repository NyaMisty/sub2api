package service

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	openAIAccountStateUpdateTimeout       = 5 * time.Second
	openAIOAuth429FallbackCooldown        = 5 * time.Second
	openAIStopSchedulingBridgeCooldown    = 2 * time.Minute
	openAIOAuth429StormWindow             = 10 * time.Second
	openAIOAuth429StormThreshold          = 20
	openAIOAuth429StormMaxAccountSwitches = 1
)

func openAIAccountStateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, openAIAccountStateUpdateTimeout)
}

func isOpenAIOAuthAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth
}

func isOpenAIAccount(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI
}

func (s *OpenAIGatewayService) handleOpenAIAccountUpstreamError(ctx context.Context, account *Account, statusCode int, headers http.Header, responseBody []byte, requestedModel ...string) bool {
	stateCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	upstreamMsg := summarizeOpenAIRuntimeBlockUpstreamError(responseBody)

	if statusCode == http.StatusTooManyRequests {
		s.markOpenAIOAuth429RateLimited(stateCtx, account, headers, responseBody, upstreamMsg)
	}
	if s == nil || account == nil || s.rateLimitService == nil {
		return false
	}
	if len(requestedModel) > 0 && s.rateLimitService.HandleUpstreamModelNotFound(stateCtx, account, requestedModel[0], statusCode, responseBody) {
		return true
	}
	shouldDisable := s.rateLimitService.HandleUpstreamError(stateCtx, account, statusCode, headers, responseBody)
	if shouldDisable {
		logger.FromContext(stateCtx).Warn("openai.runtime_block_triggered",
			zap.String("component", "service.openai_gateway"),
			zap.Int64("account_id", account.ID),
			zap.String("account_type", account.Type),
			zap.Int("upstream_status", statusCode),
			zap.String("upstream_error_message", upstreamMsg),
			zap.String("reason", "upstream_disable"),
			zap.Bool("persistent_state_expected", true),
		)
		s.BlockAccountSchedulingWithContext(stateCtx, account, time.Time{}, "upstream_disable")
	}
	return shouldDisable
}

func (s *OpenAIGatewayService) markOpenAIOAuth429RateLimited(ctx context.Context, account *Account, headers http.Header, responseBody []byte, upstreamMsg string) {
	if s == nil || !isOpenAIOAuthAccount(account) {
		return
	}
	s.recordOpenAIOAuth429()

	cooldownUntil := time.Now().Add(openAIOAuth429FallbackCooldown)
	if s.rateLimitService != nil {
		if resetAt := s.rateLimitService.calculateOpenAI429ResetTime(headers); resetAt != nil && resetAt.After(time.Now()) {
			cooldownUntil = *resetAt
		} else if resetUnix := parseOpenAIRateLimitResetTime(responseBody); resetUnix != nil {
			if resetAt := time.Unix(*resetUnix, 0); resetAt.After(time.Now()) {
				cooldownUntil = resetAt
			}
		} else if cooldown, ok := s.rateLimitService.get429FallbackCooldown(ctx, account); ok && cooldown > 0 {
			cooldownUntil = time.Now().Add(cooldown)
		}
	}
	logger.FromContext(ctx).Warn("openai.runtime_block_triggered",
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", account.ID),
		zap.String("account_type", account.Type),
		zap.Int("upstream_status", http.StatusTooManyRequests),
		zap.String("upstream_error_message", upstreamMsg),
		zap.String("reason", "429"),
		zap.Time("requested_until", cooldownUntil),
		zap.Bool("persistent_state_expected", true),
	)
	s.BlockAccountSchedulingWithContext(ctx, account, cooldownUntil, "429")
}

func summarizeOpenAIRuntimeBlockUpstreamError(body []byte) string {
	msg := strings.TrimSpace(ExtractUpstreamErrorMessage(body))
	if msg == "" {
		msg = strings.TrimSpace(string(body))
	}
	if msg == "" {
		return ""
	}
	msg = sanitizeUpstreamErrorMessage(msg)
	return truncateForLog([]byte(msg), 512)
}

func derefAccountTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func (s *OpenAIGatewayService) BlockAccountScheduling(account *Account, until time.Time, reason string) {
	s.BlockAccountSchedulingWithContext(context.Background(), account, until, reason)
}

func (s *OpenAIGatewayService) BlockAccountSchedulingWithContext(ctx context.Context, account *Account, until time.Time, reason string) {
	if s == nil || !isOpenAIAccount(account) {
		return
	}
	now := time.Now()
	requestedUntil := until
	blockUntil := requestedUntil
	if blockUntil.IsZero() || !blockUntil.After(now) {
		blockUntil = now.Add(openAIStopSchedulingBridgeCooldown)
	}
	log := logger.FromContext(ctx).With(
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", account.ID),
		zap.String("account_type", account.Type),
		zap.String("reason", reason),
		zap.Time("requested_until", requestedUntil),
		zap.Time("applied_until", blockUntil),
		zap.Bool("normalized_until", !requestedUntil.Equal(blockUntil)),
	)

	for {
		current, loaded := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
		if !loaded {
			actual, stored := s.openaiAccountRuntimeBlockUntil.LoadOrStore(account.ID, blockUntil)
			if !stored {
				log.Warn("openai.runtime_block_applied",
					zap.String("action", "store"),
				)
				return
			}
			current = actual
		}

		currentUntil, ok := current.(time.Time)
		if !ok || currentUntil.IsZero() {
			if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
				log.Warn("openai.runtime_block_applied",
					zap.String("action", "replace_invalid"),
					zap.Time("previous_until", time.Time{}),
				)
				return
			}
			continue
		}
		if currentUntil.After(blockUntil) {
			log.Info("openai.runtime_block_skipped",
				zap.String("action", "keep_existing"),
				zap.Time("previous_until", currentUntil),
			)
			return
		}
		if s.openaiAccountRuntimeBlockUntil.CompareAndSwap(account.ID, current, blockUntil) {
			action := "extend"
			if currentUntil.Equal(blockUntil) {
				action = "refresh"
			}
			log.Warn("openai.runtime_block_applied",
				zap.String("action", action),
				zap.Time("previous_until", currentUntil),
			)
			return
		}
	}
}

func (s *OpenAIGatewayService) ClearAccountSchedulingBlock(accountID int64) {
	s.ClearAccountSchedulingBlockWithContext(context.Background(), accountID)
}

func (s *OpenAIGatewayService) ClearAccountSchedulingBlockWithContext(ctx context.Context, accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	defer s.publishUserWaitWakeAfterAccountRecovery(ctx)

	log := logger.FromContext(ctx).With(
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", accountID),
	)
	current, ok := s.openaiAccountRuntimeBlockUntil.Load(accountID)
	s.openaiAccountRuntimeBlockUntil.Delete(accountID)
	if !ok {
		log.Info("openai.runtime_block_cleared", zap.String("action", "noop_missing"))
		return
	}
	if currentUntil, ok := current.(time.Time); ok && !currentUntil.IsZero() {
		log.Info("openai.runtime_block_cleared",
			zap.String("action", "delete"),
			zap.Time("previous_until", currentUntil),
		)
		return
	}
	log.Warn("openai.runtime_block_cleared",
		zap.String("action", "delete_invalid"),
	)
}

func (s *OpenAIGatewayService) publishUserWaitWakeAfterAccountRecovery(ctx context.Context) {
	if s == nil || s.concurrencyService == nil {
		return
	}
	wakeCtx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	s.concurrencyService.PublishUserWaitWake(wakeCtx)
}

func (s *OpenAIGatewayService) isOpenAIAccountRuntimeBlocked(account *Account) bool {
	return s.isOpenAIAccountRuntimeBlockedWithContext(context.Background(), account)
}

func (s *OpenAIGatewayService) isOpenAIAccountRuntimeBlockedWithContext(ctx context.Context, account *Account) bool {
	if s == nil || !isOpenAIAccount(account) {
		return false
	}
	value, ok := s.openaiAccountRuntimeBlockUntil.Load(account.ID)
	if !ok {
		return false
	}
	cooldownUntil, ok := value.(time.Time)
	if !ok || cooldownUntil.IsZero() {
		logger.FromContext(ctx).Warn("openai.runtime_block_entry_invalid",
			zap.String("component", "service.openai_gateway"),
			zap.Int64("account_id", account.ID),
		)
		s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
		s.publishUserWaitWakeAfterAccountRecovery(ctx)
		return false
	}
	if time.Now().Before(cooldownUntil) {
		return true
	}
	logger.FromContext(ctx).Info("openai.runtime_block_expired",
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", account.ID),
		zap.Time("expired_until", cooldownUntil),
	)
	s.openaiAccountRuntimeBlockUntil.Delete(account.ID)
	s.publishUserWaitWakeAfterAccountRecovery(ctx)
	return false
}

func (s *OpenAIGatewayService) recordOpenAIOAuth429() {
	if s == nil {
		return
	}
	now := time.Now()
	windowStart := s.openaiOAuth429WindowStartUnixNano.Load()
	if windowStart == 0 || now.Sub(time.Unix(0, windowStart)) >= openAIOAuth429StormWindow {
		if s.openaiOAuth429WindowStartUnixNano.CompareAndSwap(windowStart, now.UnixNano()) {
			s.openaiOAuth429WindowCount.Store(1)
			return
		}
	}
	s.openaiOAuth429WindowCount.Add(1)
}

func (s *OpenAIGatewayService) isOpenAIOAuth429Storm() bool {
	if s == nil {
		return false
	}
	windowStart := s.openaiOAuth429WindowStartUnixNano.Load()
	if windowStart == 0 || time.Since(time.Unix(0, windowStart)) >= openAIOAuth429StormWindow {
		return false
	}
	return s.openaiOAuth429WindowCount.Load() >= openAIOAuth429StormThreshold
}

func (s *OpenAIGatewayService) ShouldStopOpenAIOAuth429Failover(account *Account, statusCode int, failedSwitches int) bool {
	if statusCode != http.StatusTooManyRequests || failedSwitches < openAIOAuth429StormMaxAccountSwitches {
		return false
	}
	if !isOpenAIOAuthAccount(account) {
		return false
	}
	return s.isOpenAIOAuth429Storm()
}
