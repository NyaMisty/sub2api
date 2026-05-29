//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestGatewayServiceHasPotentialAccountForRequest(t *testing.T) {
	t.Parallel()

	groupID := int64(11)
	now := time.Now()
	future := now.Add(5 * time.Minute)

	repo := &mockAccountRepoForPlatform{
		accounts: []Account{
			{
				ID:          1,
				Platform:    PlatformAnthropic,
				Status:      StatusActive,
				Schedulable: true,
				Credentials: map[string]any{
					"model_mapping": map[string]any{
						"claude-3-5-sonnet-20241022": "claude-3-5-sonnet-20241022",
					},
				},
				RateLimitResetAt: &future,
				AccountGroups:    []AccountGroup{{AccountID: 1, GroupID: groupID}},
			},
		},
		accountsByID: map[int64]*Account{},
	}
	for i := range repo.accounts {
		repo.accountsByID[repo.accounts[i].ID] = &repo.accounts[i]
	}

	groupRepo := &mockGroupRepoForGateway{
		groups: map[int64]*Group{
			groupID: {ID: groupID, Platform: PlatformAnthropic},
		},
	}

	svc := &GatewayService{
		accountRepo: repo,
		groupRepo:   groupRepo,
		cfg:         testConfig(),
	}

	hasPotential, err := svc.HasPotentialAccountForRequest(context.Background(), &groupID, "claude-3-5-sonnet-20241022")
	require.NoError(t, err)
	require.True(t, hasPotential, "temporarily rate-limited supported model should remain retryable")

	hasPotential, err = svc.HasPotentialAccountForRequest(context.Background(), &groupID, "claude-3-5-haiku-20241022")
	require.NoError(t, err)
	require.False(t, hasPotential, "unsupported model should fail fast instead of waiting")
}

func TestOpenAIGatewayServiceHasPotentialAccountForSelection(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(5 * time.Minute)
	repo := schedulerTestOpenAIAccountRepo{
		accounts: []Account{
			{
				ID:          1,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Schedulable: true,
				Credentials: map[string]any{
					"model_mapping": map[string]any{
						"text-embedding-3-small": "text-embedding-3-small",
					},
				},
				RateLimitResetAt: &future,
			},
			{
				ID:          2,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Status:      StatusActive,
				Schedulable: true,
				Credentials: map[string]any{
					"model_mapping": map[string]any{
						"gpt-5": "gpt-5",
					},
				},
			},
		},
	}

	svc := &OpenAIGatewayService{
		accountRepo: repo,
		cfg:         &config.Config{},
	}

	hasPotential, err := svc.HasPotentialAccountForSelection(
		context.Background(),
		nil,
		"text-embedding-3-small",
		OpenAIUpstreamTransportHTTPSSE,
		OpenAIEndpointCapabilityEmbeddings,
		"",
		false,
	)
	require.NoError(t, err)
	require.True(t, hasPotential, "supported embeddings account should remain retryable even if temporarily rate-limited")

	hasPotential, err = svc.HasPotentialAccountForSelection(
		context.Background(),
		nil,
		"gpt-5",
		OpenAIUpstreamTransportHTTPSSE,
		OpenAIEndpointCapabilityEmbeddings,
		"",
		false,
	)
	require.NoError(t, err)
	require.False(t, hasPotential, "when model support and required capability never overlap on the same account, the request should fail fast")
}
