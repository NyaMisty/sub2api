//go:build e2e

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type e2eUserPayload struct {
	ID int64 `json:"id"`
}

type e2eUserConcurrencyStatsPayload struct {
	Enabled bool                                `json:"enabled"`
	User    map[string]e2eUserConcurrencyRecord `json:"user"`
}

type e2eUserConcurrencyRecord struct {
	UserID         int64 `json:"user_id"`
	WaitingInQueue int64 `json:"waiting_in_queue"`
}

type qosRequestResult struct {
	label    string
	started  time.Time
	finished time.Time
	status   int
	body     string
	err      error
}

func TestUserWaitAuthorityQueue_NoAvailableWaitVisibleAndRecovers(t *testing.T) {
	adminEmail := strings.TrimSpace(getEnv("ADMIN_EMAIL", "admin@sub2api.local"))
	adminPassword := strings.TrimSpace(getEnv("ADMIN_PASSWORD", ""))
	if adminPassword == "" {
		t.Skip("未设置 ADMIN_PASSWORD，无法执行 authority queue E2E")
	}

	adminToken := e2eLoginAdmin(t, adminEmail, adminPassword)
	suffix := time.Now().UnixNano()

	groupID := e2eCreateGroup(t, adminToken, fmt.Sprintf("e2e-user-qos-%d", suffix))
	accountID := e2eCreateInterceptWarmupAccount(t, adminToken, groupID, fmt.Sprintf("e2e-user-qos-account-%d", suffix))
	e2eSetAccountSchedulable(t, adminToken, accountID, false)

	userPassword := "E2eUser@12345"
	lowUserID := e2eCreateUserWithQueuePriority(
		t,
		adminToken,
		fmt.Sprintf("e2e-low-%d@test.local", suffix),
		userPassword,
		fmt.Sprintf("e2e-low-%d", suffix),
		50,
	)
	highUserID := e2eCreateUserWithQueuePriority(
		t,
		adminToken,
		fmt.Sprintf("e2e-high-%d@test.local", suffix),
		userPassword,
		fmt.Sprintf("e2e-high-%d", suffix),
		10,
	)

	lowToken := e2eLoginUser(t, fmt.Sprintf("e2e-low-%d@test.local", suffix), userPassword)
	highToken := e2eLoginUser(t, fmt.Sprintf("e2e-high-%d@test.local", suffix), userPassword)
	lowKey := e2eCreateUserAPIKey(t, lowToken, groupID, fmt.Sprintf("e2e-low-key-%d", suffix))
	highKey := e2eCreateUserAPIKey(t, highToken, groupID, fmt.Sprintf("e2e-high-key-%d", suffix))

	results := make(chan qosRequestResult, 2)
	sendWarmup := func(label, apiKey string) {
		payload := map[string]any{
			"model":      "claude-sonnet-4-5",
			"max_tokens": 32,
			"messages": []map[string]any{
				{
					"role": "user",
					"content": []map[string]any{
						{
							"type": "text",
							"text": "Warmup",
						},
					},
				},
			},
		}
		body, err := json.Marshal(payload)
		if err != nil {
			results <- qosRequestResult{label: label, err: err}
			return
		}

		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		if err != nil {
			results <- qosRequestResult{label: label, err: err}
			return
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("anthropic-version", "2023-06-01")

		client := &http.Client{Timeout: 25 * time.Second}
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			results <- qosRequestResult{label: label, started: start, finished: time.Now(), err: err}
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		results <- qosRequestResult{
			label:    label,
			started:  start,
			finished: time.Now(),
			status:   resp.StatusCode,
			body:     strings.TrimSpace(string(respBody)),
		}
	}

	go sendWarmup("low", lowKey)
	time.Sleep(200 * time.Millisecond)
	go sendWarmup("high", highKey)

	requireQueuedUsers := func() {
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			stats := e2eGetUserConcurrencyStats(t, adminToken)
			lowWaiting := stats[strconv.FormatInt(lowUserID, 10)].WaitingInQueue
			highWaiting := stats[strconv.FormatInt(highUserID, 10)].WaitingInQueue
			if lowWaiting > 0 && highWaiting > 0 {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("expected both users to be visible in waiting queue before recovery")
	}
	requireQueuedUsers()

	time.Sleep(1 * time.Second)
	e2eSetAccountSchedulable(t, adminToken, accountID, true)

	first := <-results
	second := <-results

	var lowResult, highResult qosRequestResult
	if first.label == "low" {
		lowResult = first
		highResult = second
	} else {
		highResult = first
		lowResult = second
	}

	if highResult.err != nil {
		t.Fatalf("high-priority request failed: %v", highResult.err)
	}
	if lowResult.err != nil {
		t.Fatalf("low-priority request failed: %v", lowResult.err)
	}
	if highResult.status != http.StatusOK {
		t.Fatalf("expected high-priority request to finish with HTTP 200, got %d: %s", highResult.status, highResult.body)
	}
	if lowResult.status != http.StatusOK {
		t.Fatalf("expected low-priority request to finish with HTTP 200, got %d: %s", lowResult.status, lowResult.body)
	}
	if highResult.finished.Before(lowResult.finished) {
		t.Logf("high-priority request finished first: high=%s low=%s", highResult.finished.Format(time.RFC3339Nano), lowResult.finished.Format(time.RFC3339Nano))
	} else {
		t.Logf("low-priority request finished first in this run: high=%s low=%s", highResult.finished.Format(time.RFC3339Nano), lowResult.finished.Format(time.RFC3339Nano))
	}

	stats := e2eGetUserConcurrencyStats(t, adminToken)
	if rec, ok := stats[strconv.FormatInt(lowUserID, 10)]; ok && rec.WaitingInQueue != 0 {
		t.Fatalf("expected low-priority user waiting_in_queue to clear, got %d", rec.WaitingInQueue)
	}
	if rec, ok := stats[strconv.FormatInt(highUserID, 10)]; ok && rec.WaitingInQueue != 0 {
		t.Fatalf("expected high-priority user waiting_in_queue to clear, got %d", rec.WaitingInQueue)
	}
}

func e2eCreateInterceptWarmupAccount(t *testing.T, adminToken string, groupID int64, accountName string) int64 {
	t.Helper()

	payload := map[string]any{
		"name":     accountName,
		"platform": "anthropic",
		"type":     "oauth",
		"credentials": map[string]any{
			"access_token":              "fake-access-token",
			"intercept_warmup_requests": true,
		},
		"concurrency": 1,
		"priority":    1,
		"group_ids":   []int64{groupID},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal intercept account payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/admin/accounts", body, adminToken)
	if err != nil {
		t.Fatalf("create intercept account request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create intercept account failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal create intercept account response: %v", err)
	}

	var account e2eAccountPayload
	if err := json.Unmarshal(env.Data, &account); err != nil {
		t.Fatalf("unmarshal intercept account data: %v", err)
	}
	if account.ID <= 0 {
		t.Fatalf("invalid intercept account id in response: %s", string(respBody))
	}
	return account.ID
}

func e2eCreateUserWithQueuePriority(
	t *testing.T,
	adminToken string,
	email string,
	password string,
	username string,
	queuePriority int,
) int64 {
	t.Helper()

	payload := map[string]any{
		"email":          email,
		"password":       password,
		"username":       username,
		"balance":        100,
		"concurrency":    1,
		"queue_priority": queuePriority,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal create user payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/admin/users", body, adminToken)
	if err != nil {
		t.Fatalf("create user request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create user failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal create user response: %v", err)
	}

	var user e2eUserPayload
	if err := json.Unmarshal(env.Data, &user); err != nil {
		t.Fatalf("unmarshal create user data: %v", err)
	}
	if user.ID <= 0 {
		t.Fatalf("invalid user id in response: %s", string(respBody))
	}
	return user.ID
}

func e2eGetUserConcurrencyStats(t *testing.T, adminToken string) map[string]e2eUserConcurrencyRecord {
	t.Helper()

	resp, err := doRequest(t, http.MethodGet, "/api/v1/admin/ops/user-concurrency", nil, adminToken)
	if err != nil {
		t.Fatalf("get user concurrency stats request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get user concurrency stats failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal user concurrency stats response: %v", err)
	}

	var payload e2eUserConcurrencyStatsPayload
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		t.Fatalf("unmarshal user concurrency stats data: %v", err)
	}
	return payload.User
}
