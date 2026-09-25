// Copyright 2026 Argon Inc. LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"

	"github.com/livekit/livekit-server/pkg/config"
)

var webhookRouteTestKeys = map[string]string{
	"APIdefault": "default-secret-0123456789abcdef0123",
	"APImeet":    "meet-secret-0123456789abcdef0123456",
}

func webhookRouteTestConfig(routes ...config.WebHookRouteConfig) config.WebHookConfig {
	conf := config.WebHookConfig{WebHookConfig: webhook.DefaultWebHookConfig}
	conf.APIKey = "APIdefault"
	conf.URLs = []string{"http://127.0.0.1:1/default"}
	conf.Routes = routes
	return conf
}

func TestNewWebhookNotifier(t *testing.T) {
	kp := auth.NewFileBasedKeyProviderFromMap(webhookRouteTestKeys)
	store := NewLocalStore()
	meetRoute := config.WebHookRouteConfig{APIKey: "APImeet", URLs: []string{"http://127.0.0.1:1/meet"}}

	t.Run("without routes the default notifier is used as is", func(t *testing.T) {
		n, err := NewWebhookNotifier(webhookRouteTestConfig(), kp, store)
		require.NoError(t, err)
		defer n.Stop(true)
		require.IsType(t, &webhook.DefaultNotifier{}, n)
	})

	t.Run("routes wrap the default notifier", func(t *testing.T) {
		n, err := NewWebhookNotifier(webhookRouteTestConfig(meetRoute), kp, store)
		require.NoError(t, err)
		defer n.Stop(true)
		_, isDefault := n.(*webhook.DefaultNotifier)
		require.False(t, isDefault)
	})

	t.Run("a route api_key must be a configured key", func(t *testing.T) {
		_, err := NewWebhookNotifier(webhookRouteTestConfig(config.WebHookRouteConfig{APIKey: "APIunknown", URLs: meetRoute.URLs}), kp, store)
		require.ErrorIs(t, err, ErrWebHookRouteUnknownAPIKey)

		_, err = NewWebhookNotifier(webhookRouteTestConfig(config.WebHookRouteConfig{URLs: meetRoute.URLs}), kp, store)
		require.ErrorIs(t, err, ErrWebHookRouteUnknownAPIKey)
	})

	t.Run("route api_keys must be unique", func(t *testing.T) {
		_, err := NewWebhookNotifier(webhookRouteTestConfig(meetRoute, meetRoute), kp, store)
		require.ErrorIs(t, err, ErrWebHookRouteDuplicateAPIKey)
	})

	t.Run("a route needs urls", func(t *testing.T) {
		_, err := NewWebhookNotifier(webhookRouteTestConfig(config.WebHookRouteConfig{APIKey: "APImeet"}), kp, store)
		require.ErrorIs(t, err, ErrWebHookRouteMissingURLs)
	})
}

func TestRoomAPIKeyResolver(t *testing.T) {
	ctx := context.Background()
	store := NewLocalStore()
	resolve := roomAPIKeyResolver(store)

	require.NoError(t, store.StoreRoom(ctx, &livekit.Room{Sid: "RM_tagged", Name: "tagged"}, &livekit.RoomInternal{Tags: map[string]string{RoomAPIKeyTag: "APImeet"}}))
	require.NoError(t, store.StoreRoom(ctx, &livekit.Room{Sid: "RM_legacy", Name: "legacy"}, &livekit.RoomInternal{}))

	apiKey, roomID, found, err := resolve(ctx, "tagged")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "APImeet", apiKey)
	require.EqualValues(t, "RM_tagged", roomID)

	apiKey, roomID, found, err = resolve(ctx, "legacy")
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, apiKey)
	require.EqualValues(t, "RM_legacy", roomID)

	_, _, found, err = resolve(ctx, "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestTagRoomAPIKey(t *testing.T) {
	ctx := WithAPIKey(context.Background(), &auth.ClaimGrants{}, "APImeet")

	// the caller's key replaces a client supplied one, other tags are kept
	req := &livekit.CreateRoomRequest{Name: "room", Tags: map[string]string{RoomAPIKeyTag: "APIspoofed", "team": "a"}}
	require.Same(t, req, tagRoomAPIKey(ctx, req))
	require.Equal(t, map[string]string{RoomAPIKeyTag: "APImeet", "team": "a"}, req.Tags)

	// without a caller key a client supplied tag is dropped
	tagRoomAPIKey(context.Background(), req)
	require.Equal(t, map[string]string{"team": "a"}, req.Tags)

	req = &livekit.CreateRoomRequest{Name: "room"}
	tagRoomAPIKey(ctx, req)
	require.Equal(t, map[string]string{RoomAPIKeyTag: "APImeet"}, req.Tags)
}

func TestApplyRoomAPIKeyTag(t *testing.T) {
	internal := &livekit.RoomInternal{}
	applyRoomAPIKeyTag(&livekit.CreateRoomRequest{Tags: map[string]string{RoomAPIKeyTag: "APImeet"}}, internal)
	require.Equal(t, "APImeet", internal.Tags[RoomAPIKeyTag])

	// a room keeps the key it was created with
	applyRoomAPIKeyTag(&livekit.CreateRoomRequest{Tags: map[string]string{RoomAPIKeyTag: "APIbots"}}, internal)
	require.Equal(t, "APImeet", internal.Tags[RoomAPIKeyTag])

	// untagged requests and rooms without internal state are left alone
	applyRoomAPIKeyTag(&livekit.CreateRoomRequest{}, internal)
	require.Equal(t, "APImeet", internal.Tags[RoomAPIKeyTag])
	applyRoomAPIKeyTag(&livekit.CreateRoomRequest{Tags: map[string]string{RoomAPIKeyTag: "APIbots"}}, nil)
}
