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
	"errors"
	"fmt"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/telemetry"
)

// RoomAPIKeyTag is the room tag holding the API key a room was created with. Webhooks of
// the room are routed by it, see config.WebHookConfig.Routes.
const RoomAPIKeyTag = "lk.api_key"

// NewWebhookNotifier returns the notifier of the default route, wrapped in a router when
// per-API-key routes are configured.
func NewWebhookNotifier(conf config.WebHookConfig, kp auth.KeyProvider, store ServiceStore) (webhook.QueuedNotifier, error) {
	defaultNotifier, err := webhook.NewDefaultNotifier(conf.WebHookConfig, kp)
	if err != nil {
		return nil, err
	}
	if len(conf.Routes) == 0 {
		return defaultNotifier, nil
	}

	routes := make(map[string]webhook.QueuedNotifier, len(conf.Routes))
	for _, route := range conf.Routes {
		n, err := newWebhookRouteNotifier(conf.WebHookConfig, route, routes, kp)
		if err != nil {
			defaultNotifier.Stop(true)
			for _, n := range routes {
				n.Stop(true)
			}
			return nil, err
		}
		routes[route.APIKey] = n
	}
	return telemetry.NewWebhookRouter(defaultNotifier, routes, roomAPIKeyResolver(store)), nil
}

func newWebhookRouteNotifier(base webhook.WebHookConfig, route config.WebHookRouteConfig, routes map[string]webhook.QueuedNotifier, kp auth.KeyProvider) (webhook.QueuedNotifier, error) {
	if route.APIKey == "" || kp.GetSecret(route.APIKey) == "" {
		return nil, fmt.Errorf("%w: %q", ErrWebHookRouteUnknownAPIKey, route.APIKey)
	}
	if _, ok := routes[route.APIKey]; ok {
		return nil, fmt.Errorf("%w: %q", ErrWebHookRouteDuplicateAPIKey, route.APIKey)
	}
	if len(route.URLs) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrWebHookRouteMissingURLs, route.APIKey)
	}

	// routes share the delivery settings of the default route
	base.APIKey = route.APIKey
	base.URLs = route.URLs
	return webhook.NewDefaultNotifier(base, kp)
}

// roomAPIKeyResolver reads the API key a room was created with from the room store.
func roomAPIKeyResolver(store ServiceStore) telemetry.RoomAPIKeyResolver {
	return func(ctx context.Context, roomName livekit.RoomName) (string, livekit.RoomID, bool, error) {
		room, internal, err := store.LoadRoom(ctx, roomName, true)
		if errors.Is(err, ErrRoomNotFound) {
			return "", "", false, nil
		} else if err != nil {
			return "", "", false, err
		}
		return internal.GetTags()[RoomAPIKeyTag], livekit.RoomID(room.Sid), true, nil
	}
}

// tagRoomAPIKey records the caller's API key on a create room request, replacing any
// client supplied tag, and returns the request.
func tagRoomAPIKey(ctx context.Context, req *livekit.CreateRoomRequest) *livekit.CreateRoomRequest {
	apiKey := GetAPIKey(ctx)
	if apiKey == "" {
		delete(req.Tags, RoomAPIKeyTag)
		return req
	}
	if req.Tags == nil {
		req.Tags = make(map[string]string, 1)
	}
	req.Tags[RoomAPIKeyTag] = apiKey
	return req
}

// applyRoomAPIKeyTag copies the API key tag to a room that has none yet, so a room keeps
// the key it was created with whatever keys later requests use.
func applyRoomAPIKeyTag(req *livekit.CreateRoomRequest, internal *livekit.RoomInternal) {
	apiKey := req.GetTags()[RoomAPIKeyTag]
	if apiKey == "" || internal == nil || internal.Tags[RoomAPIKeyTag] != "" {
		return
	}
	if internal.Tags == nil {
		internal.Tags = make(map[string]string, 1)
	}
	internal.Tags[RoomAPIKeyTag] = apiKey
}
