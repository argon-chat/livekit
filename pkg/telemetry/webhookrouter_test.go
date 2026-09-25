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

package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"

	"github.com/livekit/livekit-server/pkg/telemetry"
)

const (
	routerDefaultKey = "APIdefault"
	routerMeetKey    = "APImeet"
	routerBotsKey    = "APIbots"
)

var routerKeys = map[string]string{
	routerDefaultKey: "default-secret-0123456789abcdef0123",
	routerMeetKey:    "meet-secret-0123456789abcdef0123456",
	routerBotsKey:    "bots-secret-0123456789abcdef0123456",
}

// webhookSink accepts only events signed with its API key
type webhookSink struct {
	server   *httptest.Server
	mu       sync.Mutex
	received []string
	rejected atomic.Int32
}

func newWebhookSink(t *testing.T, apiKey string) *webhookSink {
	s := &webhookSink{}
	kp := auth.NewSimpleKeyProvider(apiKey, routerKeys[apiKey])
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event, err := webhook.ReceiveWebhookEvent(r, kp)
		if err != nil {
			s.rejected.Inc()
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		s.mu.Lock()
		s.received = append(s.received, describeEvent(event))
		s.mu.Unlock()
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *webhookSink) events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

func describeEvent(event *livekit.WebhookEvent) string {
	room := event.GetRoom().GetName()
	if room == "" {
		room = event.GetEgressInfo().GetRoomName()
	}
	if room == "" {
		room = event.GetIngressInfo().GetRoomName()
	}
	desc := event.Event + ":" + room
	if id := event.GetEgressInfo().GetEgressId(); id != "" {
		desc += "/" + id
	}
	if id := event.GetIngressInfo().GetIngressId(); id != "" {
		desc += "/" + id
	}
	return desc
}

func newRouteNotifier(t *testing.T, kp auth.KeyProvider, apiKey, url string) webhook.QueuedNotifier {
	conf := webhook.DefaultWebHookConfig
	conf.APIKey = apiKey
	conf.URLs = []string{url}
	n, err := webhook.NewDefaultNotifier(conf, kp)
	require.NoError(t, err)
	return n
}

type storedRoom struct {
	apiKey string
	id     livekit.RoomID
}

func TestWebhookRouter(t *testing.T) {
	kp := auth.NewFileBasedKeyProviderFromMap(routerKeys)
	defaultSink := newWebhookSink(t, routerDefaultKey)
	meetSink := newWebhookSink(t, routerMeetKey)
	botsSink := newWebhookSink(t, routerBotsKey)

	// rooms as the store knows them, "" is a room created without a key
	var roomsMu sync.Mutex
	rooms := map[livekit.RoomName]storedRoom{
		"meet-room":   {routerMeetKey, "RM_meet1"},
		"bots-room":   {routerBotsKey, "RM_bots1"},
		"other-room":  {"APIother", "RM_other"},
		"legacy-room": {"", "RM_legacy"},
	}
	setRoom := func(name livekit.RoomName, room *storedRoom) {
		roomsMu.Lock()
		defer roomsMu.Unlock()
		if room == nil {
			delete(rooms, name)
		} else {
			rooms[name] = *room
		}
	}
	resolves := atomic.Int32{}
	resolver := func(_ context.Context, roomName livekit.RoomName) (string, livekit.RoomID, bool, error) {
		resolves.Inc()
		roomsMu.Lock()
		defer roomsMu.Unlock()
		room, ok := rooms[roomName]
		return room.apiKey, room.id, ok, nil
	}

	router := telemetry.NewWebhookRouter(
		newRouteNotifier(t, kp, routerDefaultKey, defaultSink.server.URL),
		map[string]webhook.QueuedNotifier{
			routerMeetKey: newRouteNotifier(t, kp, routerMeetKey, meetSink.server.URL),
			routerBotsKey: newRouteNotifier(t, kp, routerBotsKey, botsSink.server.URL),
		},
		resolver,
	)
	defer router.Stop(false)

	processed := atomic.Int32{}
	router.RegisterProcessedHook(func(context.Context, *livekit.WebhookInfo) { processed.Inc() })

	ctx := context.Background()
	notify := func(event *livekit.WebhookEvent) {
		require.NoError(t, router.QueueNotify(ctx, event))
	}
	roomEvent := func(event, room, id string) *livekit.WebhookEvent {
		return &livekit.WebhookEvent{Event: event, Room: &livekit.Room{Sid: id, Name: room}}
	}
	egressEvent := func(event, egressID, room, id string) *livekit.WebhookEvent {
		return &livekit.WebhookEvent{Event: event, EgressInfo: &livekit.EgressInfo{EgressId: egressID, RoomName: room, RoomId: id}}
	}
	ingressEvent := func(event, ingressID, room, id string) *livekit.WebhookEvent {
		info := &livekit.IngressInfo{IngressId: ingressID, RoomName: room}
		if id != "" {
			info.State = &livekit.IngressState{RoomId: id}
		}
		return &livekit.WebhookEvent{Event: event, IngressInfo: info}
	}

	// rooms are resolved once, then remembered by id
	notify(roomEvent(webhook.EventRoomStarted, "meet-room", "RM_meet1"))
	notify(roomEvent(webhook.EventParticipantJoined, "meet-room", "RM_meet1"))
	notify(egressEvent(webhook.EventEgressStarted, "EG_1", "meet-room", "RM_meet1"))
	notify(roomEvent(webhook.EventRoomStarted, "bots-room", "RM_bots1"))
	notify(ingressEvent(webhook.EventIngressStarted, "IN_1", "bots-room", "RM_bots1"))
	notify(roomEvent(webhook.EventRoomStarted, "other-room", "RM_other"))
	notify(roomEvent(webhook.EventRoomStarted, "legacy-room", "RM_legacy"))
	notify(roomEvent(webhook.EventRoomStarted, "unknown-room", "RM_unknown"))

	// the room is gone by the time room_finished and a late egress_ended are queued
	setRoom("meet-room", nil)
	notify(roomEvent(webhook.EventRoomFinished, "meet-room", "RM_meet1"))
	notify(egressEvent(webhook.EventEgressEnded, "EG_1", "meet-room", "RM_meet1"))

	// the name is reused by a room created with another key
	setRoom("meet-room", &storedRoom{routerBotsKey, "RM_meet2"})
	notify(roomEvent(webhook.EventRoomStarted, "meet-room", "RM_meet2"))

	// on another node a room is only seen through egress and ingress events, and its
	// name gets reused without a room_started passing by
	setRoom("shared", &storedRoom{routerMeetKey, "RM_shared1"})
	notify(egressEvent(webhook.EventEgressStarted, "EG_2", "shared", "RM_shared1"))
	setRoom("shared", &storedRoom{routerBotsKey, "RM_shared2"})
	notify(egressEvent(webhook.EventEgressStarted, "EG_3", "shared", "RM_shared2"))
	notify(egressEvent(webhook.EventEgressEnded, "EG_2", "shared", "RM_shared1"))
	notify(ingressEvent(webhook.EventIngressStarted, "IN_2", "shared", ""))
	notify(ingressEvent(webhook.EventIngressEnded, "IN_2", "shared", ""))

	// an event for an earlier room of a reused name, which this node never saw
	setRoom("cold", &storedRoom{routerBotsKey, "RM_cold2"})
	notify(egressEvent(webhook.EventEgressEnded, "EG_4", "cold", "RM_cold1"))

	require.Eventually(t, func() bool { return processed.Load() == 17 }, 5*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []string{
		"room_started:meet-room",
		"participant_joined:meet-room",
		"egress_started:meet-room/EG_1",
		"room_finished:meet-room",
		"egress_ended:meet-room/EG_1",
		"egress_started:shared/EG_2",
		"egress_ended:shared/EG_2",
	}, meetSink.events())
	require.ElementsMatch(t, []string{
		"room_started:bots-room",
		"ingress_started:bots-room/IN_1",
		"room_started:meet-room",
		"egress_started:shared/EG_3",
		"ingress_started:shared/IN_2",
		"ingress_ended:shared/IN_2",
	}, botsSink.events())
	require.ElementsMatch(t, []string{
		"room_started:other-room",
		"room_started:legacy-room",
		"room_started:unknown-room",
		"egress_ended:cold/EG_4",
	}, defaultSink.events())
	for _, s := range []*webhookSink{defaultSink, meetSink, botsSink} {
		require.Zero(t, s.rejected.Load())
	}
	// one resolution per room id and per event without one, none for cached rooms, rooms
	// that are not found are not cached
	require.EqualValues(t, 10, resolves.Load())
}
