package handler

import (
	"net/http"
	"testing"
	"time"

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
