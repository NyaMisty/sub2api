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

func TestOpenAIUpstream401WaitVisibleAndRecovers(t *testing.T) {
	adminEmail := strings.TrimSpace(getEnv("ADMIN_EMAIL", "admin@sub2api.local"))
	adminPassword := strings.TrimSpace(getEnv("ADMIN_PASSWORD", ""))
	if adminPassword == "" {
		t.Skip("未设置 ADMIN_PASSWORD，无法执行 upstream 401 wait E2E")
	}

	mockBaseURL := strings.TrimSpace(getEnv("MOCK_OPENAI_BASE_URL", ""))
	mockControlURL := strings.TrimSpace(getEnv("MOCK_OPENAI_CONTROL_URL", ""))
	if mockBaseURL == "" || mockControlURL == "" {
		t.Skip("未设置 MOCK_OPENAI_BASE_URL / MOCK_OPENAI_CONTROL_URL，跳过 upstream 401 wait E2E")
	}

	e2eSetMockOpenAIMode(t, mockControlURL, "401")

	adminToken := e2eLoginAdmin(t, adminEmail, adminPassword)
	suffix := time.Now().UnixNano()

	groupID := e2eCreatePlatformGroup(t, adminToken, fmt.Sprintf("e2e-openai-401-%d", suffix), "openai")
	accountID := e2eCreateOpenAICompatAccount(t, adminToken, groupID, fmt.Sprintf("e2e-openai-401-account-%d", suffix), mockBaseURL)
	e2eSetAccountSchedulable(t, adminToken, accountID, true)

	userEmail := fmt.Sprintf("e2e-openai-401-%d@test.local", suffix)
	userPassword := "E2eUser@12345"
	userID := e2eCreateUserWithQueuePriority(
		t,
		adminToken,
		userEmail,
		userPassword,
		fmt.Sprintf("e2e-openai-401-user-%d", suffix),
		10,
	)
	userToken := e2eLoginUser(t, userEmail, userPassword)
	apiKey := e2eCreateUserAPIKey(t, userToken, groupID, fmt.Sprintf("e2e-openai-401-key-%d", suffix))

	results := make(chan qosRequestResult, 1)
	go func() {
		results <- e2eSendOpenAIChatCompletion(apiKey)
	}()

	deadline := time.Now().Add(6 * time.Second)
	waitingVisible := false
	for time.Now().Before(deadline) {
		stats := e2eGetUserConcurrencyStats(t, adminToken)
		if rec, ok := stats[strconv.FormatInt(userID, 10)]; ok && rec.WaitingInQueue > 0 {
			waitingVisible = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !waitingVisible {
		t.Fatalf("expected upstream 401 request to return to user wait queue before recovery")
	}

	e2eSetMockOpenAIMode(t, mockControlURL, "ok")
	e2eClearAccountError(t, adminToken, accountID)
	e2eSetAccountSchedulable(t, adminToken, accountID, true)

	result := <-results
	if result.err != nil {
		t.Fatalf("chat completions request failed: %v", result.err)
	}
	if result.status != http.StatusOK {
		t.Fatalf("expected recovered request to return HTTP 200, got %d: %s", result.status, result.body)
	}

	elapsed := result.finished.Sub(result.started)
	if elapsed < 500*time.Millisecond {
		t.Fatalf("expected recovered request to spend noticeable time waiting, got %v", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("expected recovered request to finish before fallback timeout, got %v", elapsed)
	}

	stats := e2eGetUserConcurrencyStats(t, adminToken)
	if rec, ok := stats[strconv.FormatInt(userID, 10)]; ok && rec.WaitingInQueue != 0 {
		t.Fatalf("expected waiting_in_queue to clear after 401 recovery, got %d", rec.WaitingInQueue)
	}

	t.Logf("upstream 401 request recovered after %v with body: %s", elapsed, result.body)
}

func e2eCreatePlatformGroup(t *testing.T, adminToken, groupName, platform string) int64 {
	t.Helper()

	payload := map[string]any{
		"name":            groupName,
		"platform":        platform,
		"rate_multiplier": 1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal group payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/admin/groups", body, adminToken)
	if err != nil {
		t.Fatalf("create group request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create group failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal create group response: %v", err)
	}

	var group e2eGroupPayload
	if err := json.Unmarshal(env.Data, &group); err != nil {
		t.Fatalf("unmarshal group data: %v", err)
	}
	if group.ID <= 0 {
		t.Fatalf("invalid group id in response: %s", string(respBody))
	}
	return group.ID
}

func e2eCreateOpenAICompatAccount(t *testing.T, adminToken string, groupID int64, accountName, baseURL string) int64 {
	t.Helper()

	payload := map[string]any{
		"name":     accountName,
		"platform": "openai",
		"type":     "apikey",
		"credentials": map[string]any{
			"api_key":  "sk-mock-upstream",
			"base_url": baseURL,
		},
		"concurrency": 1,
		"priority":    1,
		"group_ids":   []int64{groupID},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal openai account payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/admin/accounts", body, adminToken)
	if err != nil {
		t.Fatalf("create openai account request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create openai account failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal create openai account response: %v", err)
	}

	var account e2eAccountPayload
	if err := json.Unmarshal(env.Data, &account); err != nil {
		t.Fatalf("unmarshal openai account data: %v", err)
	}
	if account.ID <= 0 {
		t.Fatalf("invalid openai account id in response: %s", string(respBody))
	}
	return account.ID
}

func e2eClearAccountError(t *testing.T, adminToken string, accountID int64) {
	t.Helper()

	resp, err := doRequest(t, http.MethodPost, fmt.Sprintf("/api/v1/admin/accounts/%d/clear-error", accountID), []byte("{}"), adminToken)
	if err != nil {
		t.Fatalf("clear account error request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear account error failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
}

func e2eSetMockOpenAIMode(t *testing.T, controlURL, mode string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s?mode=%s", controlURL, mode), bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("create mock control request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("mock control request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mock control returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}
}

func e2eSendOpenAIChatCompletion(apiKey string) qosRequestResult {
	payload := map[string]any{
		"model": "gpt-4o-mini",
		"messages": []map[string]string{
			{"role": "user", "content": "hello"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return qosRequestResult{label: "openai-401", err: err}
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return qosRequestResult{label: "openai-401", err: err}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return qosRequestResult{label: "openai-401", started: start, finished: time.Now(), err: err}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	return qosRequestResult{
		label:    "openai-401",
		started:  start,
		finished: time.Now(),
		status:   resp.StatusCode,
		body:     strings.TrimSpace(string(respBody)),
	}
}
