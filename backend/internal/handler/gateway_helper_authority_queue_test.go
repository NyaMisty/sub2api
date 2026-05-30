package handler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type authorityQueueCacheStub struct {
	mu sync.Mutex

	tryAcquireAllowed bool
	enqueueAllowed    bool
	pollStates        []service.UserWaitState

	tryAcquireCalls int
	acquireCalls    int
	releaseCalls    int
	enqueueCalls    int
	pollCalls       int

	completeStates []service.UserWaitState
}

func (s *authorityQueueCacheStub) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	return false, nil
}

func (s *authorityQueueCacheStub) ReleaseAccountSlot(context.Context, int64, string) error {
	return nil
}

func (s *authorityQueueCacheStub) GetAccountConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}

func (s *authorityQueueCacheStub) GetAccountConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	return map[int64]int{}, nil
}

func (s *authorityQueueCacheStub) IncrementAccountWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}

func (s *authorityQueueCacheStub) DecrementAccountWaitCount(context.Context, int64) error {
	return nil
}

func (s *authorityQueueCacheStub) GetAccountWaitingCount(context.Context, int64) (int, error) {
	return 0, nil
}

func (s *authorityQueueCacheStub) AcquireUserSlot(context.Context, int64, int, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireCalls++
	return true, nil
}

func (s *authorityQueueCacheStub) ReleaseUserSlot(context.Context, int64, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return nil
}

func (s *authorityQueueCacheStub) GetUserConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}

func (s *authorityQueueCacheStub) IncrementWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}

func (s *authorityQueueCacheStub) DecrementWaitCount(context.Context, int64) error {
	return nil
}

func (s *authorityQueueCacheStub) GetAccountsLoadBatch(context.Context, []service.AccountWithConcurrency) (map[int64]*service.AccountLoadInfo, error) {
	return map[int64]*service.AccountLoadInfo{}, nil
}

func (s *authorityQueueCacheStub) GetUsersLoadBatch(context.Context, []service.UserWithConcurrency) (map[int64]*service.UserLoadInfo, error) {
	return map[int64]*service.UserLoadInfo{}, nil
}

func (s *authorityQueueCacheStub) CleanupExpiredAccountSlots(context.Context, int64) error {
	return nil
}

func (s *authorityQueueCacheStub) CleanupStaleProcessSlots(context.Context, string) error {
	return nil
}

func (s *authorityQueueCacheStub) TryAcquireUserSlotRespectingQueue(context.Context, int64, int, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tryAcquireCalls++
	return s.tryAcquireAllowed, nil
}

func (s *authorityQueueCacheStub) EnqueueUserWait(context.Context, *service.UserWaitTicket, int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueueCalls++
	return s.enqueueAllowed, nil
}

func (s *authorityQueueCacheStub) PollUserWait(context.Context, string) (*service.UserWaitPollResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pollCalls++
	state := service.UserWaitStateGranted
	if len(s.pollStates) > 0 {
		state = s.pollStates[0]
		s.pollStates = s.pollStates[1:]
	}
	return &service.UserWaitPollResult{State: state}, nil
}

func (s *authorityQueueCacheStub) CompleteUserWait(_ context.Context, _ string, finalState service.UserWaitState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeStates = append(s.completeStates, finalState)
	return nil
}

func TestConcurrencyHelperAcquireUserSlotWithWait_UsesAuthorityQueue(t *testing.T) {
	cache := &authorityQueueCacheStub{
		tryAcquireAllowed: false,
		enqueueAllowed:    true,
		pollStates:        []service.UserWaitState{service.UserWaitStateGranted},
	}
	helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond)
	c, _ := newHelperTestContext("POST", "/v1/messages")
	streamStarted := false

	release, err := helper.AcquireUserSlotWithWait(c, 101, 2, 7, false, &streamStarted)
	require.NoError(t, err)
	require.NotNil(t, release)

	cache.mu.Lock()
	require.Equal(t, 1, cache.tryAcquireCalls)
	require.Equal(t, 1, cache.enqueueCalls)
	require.GreaterOrEqual(t, cache.pollCalls, 1)
	require.Equal(t, 1, cache.acquireCalls)
	require.Equal(t, []service.UserWaitState{service.UserWaitStateDone}, cache.completeStates)
	cache.mu.Unlock()

	release()
	cache.mu.Lock()
	require.Equal(t, 1, cache.releaseCalls)
	cache.mu.Unlock()
}

func TestSelectionRetryStateWaitForUserSlotReacquire_ReleasesAndRequeues(t *testing.T) {
	cache := &authorityQueueCacheStub{
		enqueueAllowed: true,
		pollStates:     []service.UserWaitState{service.UserWaitStateGranted},
	}
	helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond)
	c, _ := newHelperTestContext("POST", "/v1/messages")
	streamStarted := false

	state := newSelectionRetryState(200 * time.Millisecond)
	released := 0
	currentRelease := func() {
		released++
	}

	release, waited, err := state.WaitForUserSlotReacquire(
		c,
		helper,
		201,
		3,
		5,
		currentRelease,
		false,
		&streamStarted,
	)
	require.NoError(t, err)
	require.True(t, waited)
	require.NotNil(t, release)
	require.Equal(t, 1, released)

	cache.mu.Lock()
	require.Equal(t, 1, cache.enqueueCalls)
	require.GreaterOrEqual(t, cache.pollCalls, 1)
	require.Equal(t, 1, cache.acquireCalls)
	require.Equal(t, []service.UserWaitState{service.UserWaitStateDone}, cache.completeStates)
	cache.mu.Unlock()
}
