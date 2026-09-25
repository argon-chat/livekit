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

package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/webhook"
)

const (
	// rooms stay cached for late events like room_finished or egress_ended
	roomAPIKeyCacheTTL = 24 * time.Hour
	// events without a room id are cached by name only briefly, a name may be reused
	roomAPIKeyNameCacheTTL       = 2 * time.Minute
	roomAPIKeyCacheSweepInterval = 10 * time.Minute
)

// RoomAPIKeyResolver returns the API key a room was created with and the room's id,
// found is false when the room does not exist (any more).
type RoomAPIKeyResolver func(ctx context.Context, roomName livekit.RoomName) (apiKey string, roomID livekit.RoomID, found bool, err error)

// NewWebhookRouter returns a notifier that sends a room's events to the notifier of the
// API key the room was created with, and everything else to the default notifier.
func NewWebhookRouter(defaultNotifier webhook.QueuedNotifier, routes map[string]webhook.QueuedNotifier, resolver RoomAPIKeyResolver) webhook.QueuedNotifier {
	return &webhookRouter{
		defaultNotifier: defaultNotifier,
		routes:          routes,
		resolver:        resolver,
		byID:            make(map[livekit.RoomID]*roomAPIKeyEntry),
		byName:          make(map[livekit.RoomName]*roomAPIKeyEntry),
		lastSweep:       time.Now(),
	}
}

type webhookRouter struct {
	defaultNotifier webhook.QueuedNotifier
	routes          map[string]webhook.QueuedNotifier
	resolver        RoomAPIKeyResolver

	mu sync.Mutex
	// byID identifies one room, so it stays valid when the name is reused by another
	// room, byName serves the events that carry no room id
	byID      map[livekit.RoomID]*roomAPIKeyEntry
	byName    map[livekit.RoomName]*roomAPIKeyEntry
	lastSweep time.Time
}

type roomAPIKeyEntry struct {
	apiKey string
	// last use for byID, time of resolution for byName
	touched time.Time
}

func (r *webhookRouter) QueueNotify(ctx context.Context, event *livekit.WebhookEvent, opts ...webhook.NotifyOption) error {
	return r.notifierFor(ctx, event).QueueNotify(ctx, event, opts...)
}

func (r *webhookRouter) notifierFor(ctx context.Context, event *livekit.WebhookEvent) webhook.QueuedNotifier {
	if n, ok := r.routes[r.roomAPIKey(ctx, event)]; ok {
		return n
	}
	return r.defaultNotifier
}

// roomAPIKey returns the API key the event's room was created with, "" when unknown.
func (r *webhookRouter) roomAPIKey(ctx context.Context, event *livekit.WebhookEvent) string {
	roomName, roomID := webhookEventRoom(event)
	if roomName == "" {
		return ""
	}
	now := time.Now()

	r.mu.Lock()
	if roomID != "" {
		if entry := r.byID[roomID]; entry != nil {
			entry.touched = now
			r.mu.Unlock()
			return entry.apiKey
		}
	} else if entry := r.byName[roomName]; entry != nil && now.Sub(entry.touched) < roomAPIKeyNameCacheTTL {
		r.mu.Unlock()
		return entry.apiKey
	}
	r.mu.Unlock()

	// the event ctx may belong to a request that has long returned
	apiKey, currentID, found, err := r.resolver(context.WithoutCancel(ctx), roomName)
	if err != nil {
		logger.Warnw("could not resolve api key for webhook routing", err, "room", roomName, "roomID", roomID)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil || !found {
		// the room is gone, an event without an id is most likely a late one for the
		// room last known under that name
		if roomID == "" {
			if entry := r.byName[roomName]; entry != nil {
				return entry.apiKey
			}
		}
		return ""
	}

	r.sweepLocked(now)
	if currentID != "" {
		r.byID[currentID] = &roomAPIKeyEntry{apiKey: apiKey, touched: now}
	}
	if roomID == "" {
		r.byName[roomName] = &roomAPIKeyEntry{apiKey: apiKey, touched: now}
		return apiKey
	}
	if roomID != currentID {
		// an earlier room of that name, which this node never saw
		return ""
	}
	return apiKey
}

func (r *webhookRouter) sweepLocked(now time.Time) {
	if now.Sub(r.lastSweep) < roomAPIKeyCacheSweepInterval {
		return
	}
	r.lastSweep = now
	for roomID, entry := range r.byID {
		if now.Sub(entry.touched) > roomAPIKeyCacheTTL {
			delete(r.byID, roomID)
		}
	}
	for roomName, entry := range r.byName {
		if now.Sub(entry.touched) > roomAPIKeyCacheTTL {
			delete(r.byName, roomName)
		}
	}
}

func (r *webhookRouter) notifiers() []webhook.QueuedNotifier {
	all := make([]webhook.QueuedNotifier, 0, len(r.routes)+1)
	all = append(all, r.defaultNotifier)
	for _, n := range r.routes {
		all = append(all, n)
	}
	return all
}

func (r *webhookRouter) RegisterProcessedHook(hook func(ctx context.Context, whi *livekit.WebhookInfo)) {
	for _, n := range r.notifiers() {
		n.RegisterProcessedHook(hook)
	}
}

// SetKeys applies to the default notifier only, routes are bound to their API key.
func (r *webhookRouter) SetKeys(apiKey, apiSecret string) {
	r.defaultNotifier.SetKeys(apiKey, apiSecret)
}

func (r *webhookRouter) SetFilter(params webhook.FilterParams) {
	for _, n := range r.notifiers() {
		n.SetFilter(params)
	}
}

func (r *webhookRouter) Stop(force bool) {
	var wg sync.WaitGroup
	for _, n := range r.notifiers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Stop(force)
		}()
	}
	wg.Wait()
}

// webhookEventRoom returns the name and, when the event carries one, the id of the
// event's room.
func webhookEventRoom(event *livekit.WebhookEvent) (livekit.RoomName, livekit.RoomID) {
	name, id := event.GetRoom().GetName(), event.GetRoom().GetSid()
	if name == "" {
		name = event.GetEgressInfo().GetRoomName()
	}
	if id == "" {
		id = event.GetEgressInfo().GetRoomId()
	}
	if name == "" {
		name = event.GetIngressInfo().GetRoomName()
	}
	if id == "" {
		id = event.GetIngressInfo().GetState().GetRoomId()
	}
	return livekit.RoomName(name), livekit.RoomID(id)
}
