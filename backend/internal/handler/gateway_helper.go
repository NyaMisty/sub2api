package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// claudeCodeValidator is a singleton validator for Claude Code client detection
var claudeCodeValidator = service.NewClaudeCodeValidator()

// SetClaudeCodeClientContext 检查请求是否来自 Claude Code 客户端，并设置到 context 中
// 返回更新后的 context
func SetClaudeCodeClientContext(c *gin.Context, body []byte, parsedReq *service.ParsedRequest) {
	if c == nil || c.Request == nil {
		return
	}
	ua := c.GetHeader("User-Agent")
	// Fast path：非 Claude CLI UA 直接判定 false，避免热路径二次 JSON 反序列化。
	if !claudeCodeValidator.ValidateUserAgent(ua) {
		ctx := service.SetClaudeCodeClient(c.Request.Context(), false)
		c.Request = c.Request.WithContext(ctx)
		return
	}

	isClaudeCode := false
	if !strings.Contains(c.Request.URL.Path, "messages") {
		// 与 Validate 行为一致：非 messages 路径 UA 命中即可视为 Claude Code 客户端。
		isClaudeCode = true
	} else {
		// 仅在确认为 Claude CLI 且 messages 路径时再做 body 解析。
		bodyMap := claudeCodeBodyMapFromParsedRequest(parsedReq)
		if bodyMap == nil && len(body) > 0 {
			_ = json.Unmarshal(body, &bodyMap)
		}
		isClaudeCode = claudeCodeValidator.Validate(c.Request, bodyMap)
	}

	// 更新 request context
	ctx := service.SetClaudeCodeClient(c.Request.Context(), isClaudeCode)

	// 仅在确认为 Claude Code 客户端时提取版本号写入 context
	if isClaudeCode {
		if version := claudeCodeValidator.ExtractVersion(ua); version != "" {
			ctx = service.SetClaudeCodeVersion(ctx, version)
		}
	}

	c.Request = c.Request.WithContext(ctx)
}

func claudeCodeBodyMapFromParsedRequest(parsedReq *service.ParsedRequest) map[string]any {
	if parsedReq == nil {
		return nil
	}
	bodyMap := map[string]any{
		"model": parsedReq.Model,
	}
	if parsedReq.HasSystem {
		if system, ok := parsedReq.SystemValue(); ok {
			bodyMap["system"] = system
		} else {
			bodyMap["system"] = nil
		}
	}
	if parsedReq.MetadataUserID != "" {
		bodyMap["metadata"] = map[string]any{"user_id": parsedReq.MetadataUserID}
	}
	return bodyMap
}

// 并发槽位等待相关常量
//
// 性能优化说明：
// 原实现使用固定间隔（100ms）轮询并发槽位，存在以下问题：
// 1. 高并发时频繁轮询增加 Redis 压力
// 2. 固定间隔可能导致多个请求同时重试（惊群效应）
//
// 新实现使用指数退避 + 抖动算法：
// 1. 初始退避 100ms，每次乘以 1.5，最大 2s
// 2. 添加 ±20% 的随机抖动，分散重试时间点
// 3. 减少 Redis 压力，避免惊群效应
const (
	// maxConcurrencyWait 等待并发槽位的最大时间
	maxConcurrencyWait = 30 * time.Second
	// userWaitTotalTimeout 是同一请求的用户级 wait 硬上限。
	userWaitTotalTimeout = 15 * time.Second
	// userWaitDeadlineContextKey 保存请求级 wait 截止时间，供重入复用。
	userWaitDeadlineContextKey = "gateway.user_wait_deadline"
	// defaultPingInterval 流式响应等待时发送 ping 的默认间隔
	defaultPingInterval = 10 * time.Second
	// initialBackoff 初始退避时间
	initialBackoff = 100 * time.Millisecond
	// backoffMultiplier 退避时间乘数（指数退避）
	backoffMultiplier = 1.5
	// maxBackoff 最大退避时间
	maxBackoff = 2 * time.Second
)

// SSEPingFormat defines the format of SSE ping events for different platforms
type SSEPingFormat string

const (
	// SSEPingFormatClaude is the Claude/Anthropic SSE ping format
	SSEPingFormatClaude SSEPingFormat = "data: {\"type\": \"ping\"}\n\n"
	// SSEPingFormatNone indicates no ping should be sent (e.g., OpenAI has no ping spec)
	SSEPingFormatNone SSEPingFormat = ""
	// SSEPingFormatComment is an SSE comment ping for OpenAI/Codex CLI clients
	SSEPingFormatComment SSEPingFormat = ":\n\n"
)

// ConcurrencyError represents a concurrency limit error with context
type ConcurrencyError struct {
	SlotType  string
	IsTimeout bool
}

func (e *ConcurrencyError) Error() string {
	if e.IsTimeout {
		return fmt.Sprintf("timeout waiting for %s concurrency slot", e.SlotType)
	}
	return fmt.Sprintf("%s concurrency limit reached", e.SlotType)
}

// ConcurrencyHelper provides common concurrency slot management for gateway handlers
type ConcurrencyHelper struct {
	concurrencyService *service.ConcurrencyService
	pingFormat         SSEPingFormat
	pingInterval       time.Duration
}

// NewConcurrencyHelper creates a new ConcurrencyHelper
func NewConcurrencyHelper(concurrencyService *service.ConcurrencyService, pingFormat SSEPingFormat, pingInterval time.Duration) *ConcurrencyHelper {
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	return &ConcurrencyHelper{
		concurrencyService: concurrencyService,
		pingFormat:         pingFormat,
		pingInterval:       pingInterval,
	}
}

// wrapReleaseOnDone ensures release runs at most once and still triggers on context cancellation.
// 用于避免客户端断开或上游超时导致的并发槽位泄漏。
// 优化：基于 context.AfterFunc 注册回调，避免每请求额外守护 goroutine。
func wrapReleaseOnDone(ctx context.Context, releaseFunc func()) func() {
	if releaseFunc == nil {
		return nil
	}
	var once sync.Once
	var stop func() bool

	release := func() {
		once.Do(func() {
			if stop != nil {
				_ = stop()
			}
			releaseFunc()
		})
	}

	stop = context.AfterFunc(ctx, release)

	return release
}

// IncrementWaitCount increments the wait count for a user
func (h *ConcurrencyHelper) IncrementWaitCount(ctx context.Context, userID int64, maxWait int) (bool, error) {
	return h.concurrencyService.IncrementWaitCount(ctx, userID, maxWait)
}

// DecrementWaitCount decrements the wait count for a user
func (h *ConcurrencyHelper) DecrementWaitCount(ctx context.Context, userID int64) {
	h.concurrencyService.DecrementWaitCount(ctx, userID)
}

// IncrementAccountWaitCount increments the wait count for an account
func (h *ConcurrencyHelper) IncrementAccountWaitCount(ctx context.Context, accountID int64, maxWait int) (bool, error) {
	return h.concurrencyService.IncrementAccountWaitCount(ctx, accountID, maxWait)
}

// DecrementAccountWaitCount decrements the wait count for an account
func (h *ConcurrencyHelper) DecrementAccountWaitCount(ctx context.Context, accountID int64) {
	h.concurrencyService.DecrementAccountWaitCount(ctx, accountID)
}

// TryAcquireUserSlot 尝试立即获取用户并发槽位。
// 返回值: (releaseFunc, acquired, error)
func (h *ConcurrencyHelper) TryAcquireUserSlot(ctx context.Context, userID int64, maxConcurrency int) (func(), bool, error) {
	result, err := h.concurrencyService.AcquireUserSlot(ctx, userID, maxConcurrency)
	if err != nil {
		return nil, false, err
	}
	if !result.Acquired {
		return nil, false, nil
	}
	return result.ReleaseFunc, true, nil
}

func (h *ConcurrencyHelper) SupportsAuthorityUserQueue() bool {
	if h == nil || h.concurrencyService == nil {
		return false
	}
	return h.concurrencyService.SupportsUserWaitAuthorityQueue()
}

// TryAcquireAccountSlot 尝试立即获取账号并发槽位。
// 返回值: (releaseFunc, acquired, error)
func (h *ConcurrencyHelper) TryAcquireAccountSlot(ctx context.Context, accountID int64, maxConcurrency int) (func(), bool, error) {
	result, err := h.concurrencyService.AcquireAccountSlot(ctx, accountID, maxConcurrency)
	if err != nil {
		return nil, false, err
	}
	if !result.Acquired {
		return nil, false, nil
	}
	return result.ReleaseFunc, true, nil
}

// AcquireUserSlotWithWait acquires a user concurrency slot, waiting if necessary.
// For streaming requests, sends ping events during the wait.
// streamStarted is updated if streaming response has begun.
func (h *ConcurrencyHelper) AcquireUserSlotWithWait(c *gin.Context, userID int64, maxConcurrency int, queuePriority int, isStream bool, streamStarted *bool) (func(), error) {
	ctx := c.Request.Context()

	// Try to acquire immediately
	releaseFunc, acquired, err := h.TryAcquireUserSlot(ctx, userID, maxConcurrency)
	if err != nil {
		return nil, err
	}

	if acquired {
		return releaseFunc, nil
	}

	if h.SupportsAuthorityUserQueue() {
		releaseFunc, acquired, err = h.waitForUserSlotTurn(
			c,
			userID,
			maxConcurrency,
			queuePriority,
			maxConcurrencyWait,
			0,
			service.UserWaitReasonUserSlot,
			isStream,
			streamStarted,
		)
		if err != nil {
			if errors.Is(err, service.ErrUserWaitQueueFull) {
				return nil, &ConcurrencyError{SlotType: "user"}
			}
			return nil, err
		}
		if !acquired {
			return nil, &ConcurrencyError{
				SlotType:  "user",
				IsTimeout: true,
			}
		}
		return releaseFunc, nil
	}

	maxWait := service.CalculateMaxWait(maxConcurrency)
	canWait, waitErr := h.IncrementWaitCount(ctx, userID, maxWait)
	if waitErr != nil {
		// 保持原有降级语义：等待计数异常时放行后续等待流程。
	} else if !canWait {
		return nil, &ConcurrencyError{SlotType: "user"}
	}

	waitCounted := waitErr == nil && canWait
	defer func() {
		if waitCounted {
			h.DecrementWaitCount(ctx, userID)
		}
	}()

	// Need to wait - handle streaming ping if needed
	releaseFunc, err = h.waitForSlotWithPing(c, "user", userID, maxConcurrency, isStream, streamStarted)
	if err != nil {
		return nil, err
	}
	if waitCounted {
		h.DecrementWaitCount(ctx, userID)
		waitCounted = false
	}
	return releaseFunc, nil
}

func (h *ConcurrencyHelper) WaitForNoAvailableUserSlotReacquire(
	c *gin.Context,
	userID int64,
	maxConcurrency int,
	queuePriority int,
	timeout time.Duration,
	initialDelay time.Duration,
	currentReleaseFunc func(),
	isStream bool,
	streamStarted *bool,
) (func(), bool, error) {
	if currentReleaseFunc != nil {
		currentReleaseFunc()
	}
	return h.waitForUserSlotTurn(
		c,
		userID,
		maxConcurrency,
		queuePriority,
		timeout,
		initialDelay,
		service.UserWaitReasonNoAvailable,
		isStream,
		streamStarted,
	)
}

// AcquireAccountSlotWithWait acquires an account concurrency slot, waiting if necessary.
// For streaming requests, sends ping events during the wait.
// streamStarted is updated if streaming response has begun.
func (h *ConcurrencyHelper) AcquireAccountSlotWithWait(c *gin.Context, accountID int64, maxConcurrency int, isStream bool, streamStarted *bool) (func(), error) {
	ctx := c.Request.Context()

	// Try to acquire immediately
	releaseFunc, acquired, err := h.TryAcquireAccountSlot(ctx, accountID, maxConcurrency)
	if err != nil {
		return nil, err
	}

	if acquired {
		return releaseFunc, nil
	}

	// Need to wait - handle streaming ping if needed
	return h.waitForSlotWithPing(c, "account", accountID, maxConcurrency, isStream, streamStarted)
}

// waitForSlotWithPing waits for a concurrency slot, sending ping events for streaming requests.
// streamStarted pointer is updated when streaming begins (for proper error handling by caller).
func (h *ConcurrencyHelper) waitForSlotWithPing(c *gin.Context, slotType string, id int64, maxConcurrency int, isStream bool, streamStarted *bool) (func(), error) {
	return h.waitForSlotWithPingTimeout(c, slotType, id, maxConcurrency, maxConcurrencyWait, isStream, streamStarted, false)
}

// WaitWithPing waits for the given duration while keeping streaming clients alive
// with periodic ping/comment events when the endpoint supports them.
func (h *ConcurrencyHelper) WaitWithPing(c *gin.Context, wait time.Duration, isStream bool, streamStarted *bool) error {
	if wait <= 0 {
		return nil
	}
	ctx := c.Request.Context()

	needPing := isStream && h.pingFormat != ""

	var flusher http.Flusher
	if needPing {
		var ok bool
		flusher, ok = c.Writer.(http.Flusher)
		if !ok {
			return fmt.Errorf("streaming not supported")
		}
	}

	var pingCh <-chan time.Time
	if needPing {
		pingTicker := time.NewTicker(h.pingInterval)
		defer pingTicker.Stop()
		pingCh = pingTicker.C
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingCh:
			if !*streamStarted {
				c.Header("Content-Type", "text/event-stream")
				c.Header("Cache-Control", "no-cache")
				c.Header("Connection", "keep-alive")
				c.Header("X-Accel-Buffering", "no")
				*streamStarted = true
			}
			if _, err := fmt.Fprint(c.Writer, string(h.pingFormat)); err != nil {
				return err
			}
			flusher.Flush()
		case <-timer.C:
			return nil
		}
	}
}

func (h *ConcurrencyHelper) waitForUserSlotTurn(
	c *gin.Context,
	userID int64,
	maxConcurrency int,
	queuePriority int,
	timeout time.Duration,
	initialDelay time.Duration,
	reason service.UserWaitReason,
	isStream bool,
	streamStarted *bool,
) (func(), bool, error) {
	if h == nil || h.concurrencyService == nil {
		return nil, false, service.ErrUserWaitQueueUnavailable
	}
	if timeout <= 0 {
		return nil, false, nil
	}

	deadline := userWaitDeadline(c, timeout)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, false, nil
	}

	ctx := c.Request.Context()
	var wakeCh <-chan struct{}
	var wakeUnsubscribe func()
	if h.concurrencyService != nil {
		wakeCh, wakeUnsubscribe = h.concurrencyService.SubscribeUserWaitWake(ctx)
	}
	if wakeUnsubscribe != nil {
		defer wakeUnsubscribe()
	}
	ticket, err := h.concurrencyService.EnqueueUserWait(ctx, service.UserWaitRequest{
		UserID:         userID,
		MaxConcurrency: maxConcurrency,
		MaxWait:        service.CalculateMaxWait(maxConcurrency),
		Priority:       queuePriority,
		Reason:         reason,
		Timeout:        remaining,
	})
	if err != nil {
		return nil, false, err
	}
	setUserWaitDeadline(c, deadline)

	completed := false
	finish := func(state service.UserWaitState) {
		if completed || ticket == nil {
			return
		}
		completed = true
		h.concurrencyService.CompleteUserWaitBestEffort(ticket.RequestID, state)
	}
	defer func() {
		if !completed && ctx.Err() != nil {
			finish(service.UserWaitStateCanceled)
		}
	}()

	backoff := initialBackoff
	if initialDelay > 0 {
		delay := initialDelay
		if remaining := time.Until(deadline); delay > remaining {
			delay = remaining
		}
		if delay > 0 {
			woke, err := h.waitWithPingOrWake(c, delay, wakeCh, isStream, streamStarted)
			if err != nil {
				finish(service.UserWaitStateCanceled)
				return nil, false, err
			}
			if woke {
				backoff = initialBackoff
			} else {
				backoff = nextBackoff(delay)
			}
		}
	}
	for {
		poll, err := h.concurrencyService.PollUserWait(ctx, ticket.RequestID)
		if err != nil {
			finish(service.UserWaitStateCanceled)
			return nil, false, err
		}

		state := service.UserWaitStateMissing
		if poll != nil {
			state = poll.State
		}
		switch state {
		case service.UserWaitStateGranted:
			result, err := h.concurrencyService.AcquireGrantedUserSlot(ctx, userID, maxConcurrency)
			if err != nil {
				finish(service.UserWaitStateCanceled)
				return nil, false, err
			}
			if result == nil || !result.Acquired {
				finish(service.UserWaitStateCanceled)
				return nil, false, fmt.Errorf("granted user wait did not acquire slot")
			}
			finish(service.UserWaitStateDone)
			return result.ReleaseFunc, true, nil
		case service.UserWaitStateTimedOut:
			finish(service.UserWaitStateTimedOut)
			return nil, false, nil
		case service.UserWaitStateQueued:
		case service.UserWaitStateCanceled, service.UserWaitStateDone, service.UserWaitStateMissing:
			finish(state)
			if ctx.Err() != nil {
				return nil, false, ctx.Err()
			}
			return nil, false, fmt.Errorf("user wait request %s ended unexpectedly with state=%s", ticket.RequestID, state)
		default:
			finish(state)
			return nil, false, fmt.Errorf("unknown user wait state: %s", state)
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			finish(service.UserWaitStateTimedOut)
			return nil, false, nil
		}

		delay := backoff
		if delay <= 0 {
			delay = initialBackoff
		}
		if delay > remaining {
			delay = remaining
		}
		woke, err := h.waitWithPingOrWake(c, delay, wakeCh, isStream, streamStarted)
		if err != nil {
			finish(service.UserWaitStateCanceled)
			return nil, false, err
		}
		if woke {
			backoff = initialBackoff
		} else {
			backoff = nextBackoff(delay)
		}
	}
}

func userWaitDeadline(c *gin.Context, timeout time.Duration) time.Time {
	now := time.Now()
	deadline := now.Add(userWaitTotalTimeout)
	if timeout > 0 {
		timeoutDeadline := now.Add(timeout)
		if timeoutDeadline.Before(deadline) {
			deadline = timeoutDeadline
		}
	}
	if c == nil {
		return deadline
	}
	if existing, ok := c.Get(userWaitDeadlineContextKey); ok {
		if existingDeadline, ok := existing.(time.Time); ok && !existingDeadline.IsZero() && existingDeadline.Before(deadline) {
			deadline = existingDeadline
		}
	}
	return deadline
}

func setUserWaitDeadline(c *gin.Context, deadline time.Time) {
	if c == nil || deadline.IsZero() {
		return
	}
	if existing, ok := c.Get(userWaitDeadlineContextKey); ok {
		if existingDeadline, ok := existing.(time.Time); ok && !existingDeadline.IsZero() && existingDeadline.Before(deadline) {
			deadline = existingDeadline
		}
	}
	c.Set(userWaitDeadlineContextKey, deadline)
}

func (h *ConcurrencyHelper) waitWithPingOrWake(c *gin.Context, wait time.Duration, wakeCh <-chan struct{}, isStream bool, streamStarted *bool) (bool, error) {
	if wait <= 0 {
		return false, nil
	}
	if wakeCh == nil {
		return false, h.WaitWithPing(c, wait, isStream, streamStarted)
	}

	ctx := c.Request.Context()
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	needPing := isStream && h.pingFormat != ""
	var pingCh <-chan time.Time
	var pingTicker *time.Ticker
	if needPing {
		var ok bool
		_, ok = c.Writer.(http.Flusher)
		if !ok {
			return false, fmt.Errorf("streaming not supported")
		}
		pingTicker = time.NewTicker(h.pingInterval)
		defer pingTicker.Stop()
		pingCh = pingTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wakeCh:
			return true, nil
		case <-deadline.C:
			return false, nil
		case <-pingCh:
			if !*streamStarted {
				c.Header("Content-Type", "text/event-stream")
				c.Header("Cache-Control", "no-cache")
				c.Header("Connection", "keep-alive")
				c.Header("X-Accel-Buffering", "no")
				*streamStarted = true
			}
			if _, err := fmt.Fprint(c.Writer, string(h.pingFormat)); err != nil {
				return false, err
			}
			c.Writer.(http.Flusher).Flush()
		}
	}
}

// waitForSlotWithPingTimeout waits for a concurrency slot with a custom timeout.
func (h *ConcurrencyHelper) waitForSlotWithPingTimeout(c *gin.Context, slotType string, id int64, maxConcurrency int, timeout time.Duration, isStream bool, streamStarted *bool, tryImmediate bool) (func(), error) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	acquireSlot := func() (*service.AcquireResult, error) {
		if slotType == "user" {
			return h.concurrencyService.AcquireUserSlot(ctx, id, maxConcurrency)
		}
		return h.concurrencyService.AcquireAccountSlot(ctx, id, maxConcurrency)
	}

	if tryImmediate {
		result, err := acquireSlot()
		if err != nil {
			return nil, err
		}
		if result.Acquired {
			return result.ReleaseFunc, nil
		}
	}

	// Determine if ping is needed (streaming + ping format defined)
	needPing := isStream && h.pingFormat != ""

	var flusher http.Flusher
	if needPing {
		var ok bool
		flusher, ok = c.Writer.(http.Flusher)
		if !ok {
			return nil, fmt.Errorf("streaming not supported")
		}
	}

	// Only create ping ticker if ping is needed
	var pingCh <-chan time.Time
	if needPing {
		pingTicker := time.NewTicker(h.pingInterval)
		defer pingTicker.Stop()
		pingCh = pingTicker.C
	}

	backoff := initialBackoff
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if parentErr := c.Request.Context().Err(); parentErr != nil {
				return nil, parentErr
			}
			return nil, &ConcurrencyError{
				SlotType:  slotType,
				IsTimeout: true,
			}

		case <-pingCh:
			// Send ping to keep connection alive
			if !*streamStarted {
				c.Header("Content-Type", "text/event-stream")
				c.Header("Cache-Control", "no-cache")
				c.Header("Connection", "keep-alive")
				c.Header("X-Accel-Buffering", "no")
				*streamStarted = true
			}
			if _, err := fmt.Fprint(c.Writer, string(h.pingFormat)); err != nil {
				return nil, err
			}
			flusher.Flush()

		case <-timer.C:
			// Try to acquire slot
			result, err := acquireSlot()
			if err != nil {
				return nil, err
			}

			if result.Acquired {
				return result.ReleaseFunc, nil
			}
			backoff = nextBackoff(backoff)
			timer.Reset(backoff)
		}
	}
}

// AcquireAccountSlotWithWaitTimeout acquires an account slot with a custom timeout (keeps SSE ping).
func (h *ConcurrencyHelper) AcquireAccountSlotWithWaitTimeout(c *gin.Context, accountID int64, maxConcurrency int, timeout time.Duration, isStream bool, streamStarted *bool) (func(), error) {
	return h.waitForSlotWithPingTimeout(c, "account", accountID, maxConcurrency, timeout, isStream, streamStarted, true)
}

// nextBackoff 计算下一次退避时间
// 性能优化：使用指数退避 + 随机抖动，避免惊群效应
// current: 当前退避时间
// 返回值：下一次退避时间（100ms ~ 2s 之间）
func nextBackoff(current time.Duration) time.Duration {
	// 指数退避：当前时间 * 1.5
	next := time.Duration(float64(current) * backoffMultiplier)
	if next > maxBackoff {
		next = maxBackoff
	}
	// 添加 ±20% 的随机抖动（jitter 范围 0.8 ~ 1.2）
	// 抖动可以分散多个请求的重试时间点，避免同时冲击 Redis
	jitter := 0.8 + rand.Float64()*0.4
	jittered := time.Duration(float64(next) * jitter)
	if jittered < initialBackoff {
		return initialBackoff
	}
	if jittered > maxBackoff {
		return maxBackoff
	}
	return jittered
}
