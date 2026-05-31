package handler

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyHelperWaitWithPing_StreamWritesKeepAlive(t *testing.T) {
	helper := &ConcurrencyHelper{
		pingFormat:   SSEPingFormatComment,
		pingInterval: 5 * time.Millisecond,
	}
	c, rec := newHelperTestContext(http.MethodPost, "/v1/responses")
	streamStarted := false

	err := helper.WaitWithPing(c, 20*time.Millisecond, true, &streamStarted)
	require.NoError(t, err)
	require.True(t, streamStarted)
	require.Contains(t, rec.Body.String(), ":\n\n")
}

func TestSelectionRetryStateWait_StopsAfterTimeout(t *testing.T) {
	helper := &ConcurrencyHelper{
		pingFormat:   SSEPingFormatNone,
		pingInterval: time.Millisecond,
	}
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages")
	streamStarted := false
	state := &selectionRetryState{
		timeout:  30 * time.Millisecond,
		backoff:  5 * time.Millisecond,
		started:  false,
		deadline: time.Time{},
	}

	waited, err := state.Wait(c, helper, false, &streamStarted)
	require.NoError(t, err)
	require.True(t, waited)

	waited, err = state.Wait(c, helper, false, &streamStarted)
	require.NoError(t, err)
	require.True(t, waited)

	waited, err = state.Wait(c, helper, false, &streamStarted)
	require.NoError(t, err)
	require.False(t, waited)
}

func TestShouldRetryNoAvailableSelectionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error waits",
			err:  nil,
			want: true,
		},
		{
			name: "generic no available waits",
			err:  service.ErrNoAvailableAccounts,
			want: true,
		},
		{
			name: "compact no available also waits",
			err:  service.ErrNoAvailableCompactAccounts,
			want: true,
		},
		{
			name: "model unsupported no available still waits",
			err:  fmt.Errorf("no available OpenAI accounts supporting model: gpt-5.5"),
			want: true,
		},
		{
			name: "channel pricing restriction does not wait",
			err:  fmt.Errorf("%w supporting model: gpt-5.5 (channel pricing restriction)", service.ErrNoAvailableAccounts),
			want: false,
		},
		{
			name: "non no-available error does not wait",
			err:  fmt.Errorf("database timeout"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldRetryNoAvailableSelectionError(tt.err))
		})
	}
}

func TestShouldRetryWaitQueueAfterFailover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *service.UpstreamFailoverError
		want bool
	}{
		{
			name: "nil failover does not wait",
			err:  nil,
			want: false,
		},
		{
			name: "401 failover waits",
			err:  &service.UpstreamFailoverError{StatusCode: http.StatusUnauthorized},
			want: true,
		},
		{
			name: "403 failover does not wait",
			err:  &service.UpstreamFailoverError{StatusCode: http.StatusForbidden},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldRetryWaitQueueAfterFailover(tt.err))
		})
	}
}

func TestSelectionRetryStateWaitForRetryableFailoverReacquire(t *testing.T) {
	t.Run("non_retryable_failover_returns_immediately", func(t *testing.T) {
		helper := &ConcurrencyHelper{
			pingFormat:   SSEPingFormatNone,
			pingInterval: time.Millisecond,
		}
		c, _ := newHelperTestContext(http.MethodPost, "/v1/messages")
		streamStarted := false
		state := newSelectionRetryState(20 * time.Millisecond)

		release := func() {}
		gotRelease, waited, err := state.WaitForRetryableFailoverReacquire(
			c,
			helper,
			1,
			1,
			0,
			release,
			false,
			&streamStarted,
			&service.UpstreamFailoverError{StatusCode: http.StatusForbidden},
		)

		require.NoError(t, err)
		require.False(t, waited)
		require.NotNil(t, gotRelease)
	})

	t.Run("retryable_401_failover_waits", func(t *testing.T) {
		helper := &ConcurrencyHelper{
			pingFormat:   SSEPingFormatNone,
			pingInterval: time.Millisecond,
		}
		c, _ := newHelperTestContext(http.MethodPost, "/v1/messages")
		streamStarted := false
		state := &selectionRetryState{
			timeout:  25 * time.Millisecond,
			backoff:  5 * time.Millisecond,
			started:  false,
			deadline: time.Time{},
		}

		release := func() {}
		start := time.Now()
		gotRelease, waited, err := state.WaitForRetryableFailoverReacquire(
			c,
			helper,
			1,
			1,
			0,
			release,
			false,
			&streamStarted,
			&service.UpstreamFailoverError{StatusCode: http.StatusUnauthorized},
		)

		require.NoError(t, err)
		require.True(t, waited)
		require.NotNil(t, gotRelease)
		require.GreaterOrEqual(t, time.Since(start), 5*time.Millisecond)
	})
}
