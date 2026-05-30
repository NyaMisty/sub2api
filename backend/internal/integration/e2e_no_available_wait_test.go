//go:build e2e

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type e2eEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type e2eAuthPayload struct {
	AccessToken string `json:"access_token"`
}

type e2eGroupPayload struct {
	ID int64 `json:"id"`
}

type e2eAccountPayload struct {
	ID int64 `json:"id"`
}

type e2eAPIKeyPayload struct {
	Key string `json:"key"`
}

func TestNoAvailableAccountWaitsBeforeFailure(t *testing.T) {
	adminEmail := strings.TrimSpace(getEnv("ADMIN_EMAIL", "admin@sub2api.local"))
	adminPassword := strings.TrimSpace(getEnv("ADMIN_PASSWORD", ""))
	if adminPassword == "" {
		t.Skip("未设置 ADMIN_PASSWORD，无法执行 no-available-account E2E")
	}

	adminToken := e2eLoginAdmin(t, adminEmail, adminPassword)

	suffix := time.Now().UnixNano()
	groupID := e2eCreateGroup(t, adminToken, fmt.Sprintf("e2e-no-account-%d", suffix))
	accountID := e2eCreateAccount(t, adminToken, groupID, fmt.Sprintf("e2e-no-account-%d", suffix))
	e2eSetAccountSchedulable(t, adminToken, accountID, false)
	userEmail := fmt.Sprintf("e2e-no-account-%d@test.local", suffix)
	userPassword := "E2eUser@12345"
	e2eCreateUser(t, adminToken, userEmail, userPassword, fmt.Sprintf("e2e-no-account-user-%d", suffix))
	userToken := e2eLoginUser(t, userEmail, userPassword)
	apiKey := e2eCreateUserAPIKey(t, userToken, groupID, fmt.Sprintf("e2e-no-account-key-%d", suffix))

	payload := map[string]any{
		"model":      "claude-sonnet-4-5",
		"max_tokens": 16,
		"messages": []map[string]string{
			{"role": "user", "content": "hello"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal gateway payload: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create gateway request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	client := &http.Client{Timeout: 25 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("gateway request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 after wait, got %d: %s", resp.StatusCode, string(respBody))
	}
	if elapsed < 10*time.Second {
		t.Fatalf("expected request to wait about 10s before failing, got %v", elapsed)
	}
	if elapsed > 13*time.Second {
		t.Fatalf("expected request to stop shortly after 10s, got %v", elapsed)
	}
	t.Logf("request failed after %v with body: %s", elapsed, strings.TrimSpace(string(respBody)))
}

func e2eLoginAdmin(t *testing.T, email, password string) string {
	t.Helper()
	return e2eLoginUser(t, email, password)
}

func e2eCreateGroup(t *testing.T, adminToken, groupName string) int64 {
	t.Helper()

	payload := map[string]any{
		"name":            groupName,
		"platform":        "anthropic",
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

func e2eCreateAccount(t *testing.T, adminToken string, groupID int64, accountName string) int64 {
	t.Helper()

	payload := map[string]any{
		"name":        accountName,
		"platform":    "anthropic",
		"type":        "oauth",
		"credentials": map[string]any{"access_token": "fake-access-token"},
		"concurrency": 1,
		"priority":    1,
		"group_ids":   []int64{groupID},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal account payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/admin/accounts", body, adminToken)
	if err != nil {
		t.Fatalf("create account request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create account failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal create account response: %v", err)
	}

	var account e2eAccountPayload
	if err := json.Unmarshal(env.Data, &account); err != nil {
		t.Fatalf("unmarshal account data: %v", err)
	}
	if account.ID <= 0 {
		t.Fatalf("invalid account id in response: %s", string(respBody))
	}
	return account.ID
}

func e2eSetAccountSchedulable(t *testing.T, adminToken string, accountID int64, schedulable bool) {
	t.Helper()

	payload := map[string]bool{"schedulable": schedulable}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal schedulable payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, fmt.Sprintf("/api/v1/admin/accounts/%d/schedulable", accountID), body, adminToken)
	if err != nil {
		t.Fatalf("set account schedulable request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set account schedulable failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
}

func e2eCreateUserAPIKey(t *testing.T, userToken string, groupID int64, keyName string) string {
	t.Helper()

	payload := map[string]any{
		"name":     keyName,
		"group_id": groupID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal api key payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/keys", body, userToken)
	if err != nil {
		t.Fatalf("create api key request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create api key failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal api key response: %v", err)
	}

	var key e2eAPIKeyPayload
	if err := json.Unmarshal(env.Data, &key); err != nil {
		t.Fatalf("unmarshal api key data: %v", err)
	}
	if strings.TrimSpace(key.Key) == "" {
		t.Fatalf("api key response missing key: %s", string(respBody))
	}
	return key.Key
}

func e2eCreateUser(t *testing.T, adminToken, email, password, username string) {
	t.Helper()

	payload := map[string]any{
		"email":       email,
		"password":    password,
		"username":    username,
		"balance":     100,
		"concurrency": 5,
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
}

func e2eLoginUser(t *testing.T, email, password string) string {
	t.Helper()

	payload := map[string]string{
		"email":    email,
		"password": password,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal login payload: %v", err)
	}

	resp, err := doRequest(t, http.MethodPost, "/api/v1/auth/login", body, "")
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var env e2eEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		t.Fatalf("unmarshal login response: %v", err)
	}

	var auth e2eAuthPayload
	if err := json.Unmarshal(env.Data, &auth); err != nil {
		t.Fatalf("unmarshal login data: %v", err)
	}
	if strings.TrimSpace(auth.AccessToken) == "" {
		t.Fatalf("login response missing access_token: %s", string(respBody))
	}
	return auth.AccessToken
}
