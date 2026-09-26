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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/psrpc"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/audio"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
)

const forwardTestNodeID = livekit.NodeID("ND_forward_test")

// forwardTestAllocator is a minimal RoomAllocator; servicefakes cannot be used from inside package service.
type forwardTestAllocator struct {
	validateErr error
	lastReq     *livekit.CreateRoomRequest
}

func (a *forwardTestAllocator) AutoCreateEnabled(context.Context) bool { return a.validateErr == nil }

func (a *forwardTestAllocator) SelectRoomNode(context.Context, livekit.RoomName, livekit.NodeID) error {
	return nil
}

func (a *forwardTestAllocator) CreateRoom(_ context.Context, req *livekit.CreateRoomRequest, _ bool) (*livekit.Room, *livekit.RoomInternal, bool, error) {
	a.lastReq = req
	return &livekit.Room{Name: req.Name, Sid: "RM_" + req.Name}, &livekit.RoomInternal{}, true, nil
}

func (a *forwardTestAllocator) ValidateCreateRoom(context.Context, livekit.RoomName) error {
	return a.validateErr
}

type forwardTestEnv struct {
	rm        *RoomManager
	bus       psrpc.MessageBus
	router    *routingfakes.FakeRouter
	allocator *forwardTestAllocator
	client    rpc.TypedParticipantClient
}

func newForwardTestEnv(t *testing.T) *forwardTestEnv {
	prometheus.Init("test", livekit.NodeType_SERVER)

	bus := psrpc.NewLocalMessageBus()
	node, err := routing.NewLocalNodeFromNodeProto(&livekit.Node{Id: string(forwardTestNodeID)})
	require.NoError(t, err)

	router := &routingfakes.FakeRouter{}
	router.GetNodeForRoomReturns(nil, routing.ErrNotFound)

	allocator := &forwardTestAllocator{}

	conf := &config.Config{}
	conf.Room.EnableRemoteUnmute = true
	conf.Audio.UpdateInterval = 25

	rm := &RoomManager{
		config:        conf,
		rtcConfig:     &rtc.WebRTCConfig{},
		serverInfo:    &livekit.ServerInfo{NodeId: string(forwardTestNodeID)},
		currentNode:   node,
		router:        router,
		roomAllocator: allocator,
		roomStore:     NewLocalStore(),
		telemetry:     &telemetryfakes.FakeTelemetryService{},
		bus:           bus,
		rooms:         make(map[livekit.RoomName]*rtc.Room),
	}
	t.Cleanup(func() {
		rm.lock.RLock()
		rooms := make([]*rtc.Room, 0, len(rm.rooms))
		for _, room := range rm.rooms {
			rooms = append(rooms, room)
		}
		rm.lock.RUnlock()
		for _, room := range rooms {
			room.Close(types.RoomCloseReasonUnknown)
		}
		rm.participantServers.Kill()
	})

	client, err := rpc.NewTypedParticipantClient(rpc.ClientParams{Bus: bus})
	require.NoError(t, err)

	return &forwardTestEnv{rm: rm, bus: bus, router: router, allocator: allocator, client: client}
}

// newRoom creates a live room on the manager, the way getOrCreateRoom would.
func (e *forwardTestEnv) newRoom(t *testing.T, name string) *rtc.Room {
	return e.newRoomWithInternal(t, name, nil)
}

func (e *forwardTestEnv) newRoomWithInternal(t *testing.T, name string, internal *livekit.RoomInternal) *rtc.Room {
	room := rtc.NewRoom(
		&livekit.Room{Name: name, Sid: "RM_" + name},
		internal,
		rtc.WebRTCConfig{},
		config.RoomConfig{EmptyTimeout: 300, DepartureTimeout: 1},
		&sfu.AudioConfig{AudioLevelConfig: audio.AudioLevelConfig{UpdateInterval: 25}},
		&livekit.ServerInfo{},
		e.rm.telemetry,
		nil, nil, nil,
	)
	e.rm.lock.Lock()
	e.rm.rooms[livekit.RoomName(name)] = room
	e.rm.lock.Unlock()
	return room
}

func (e *forwardTestEnv) newSource(t *testing.T, room *rtc.Room, identity livekit.ParticipantIdentity) (*typesfakes.FakeLocalParticipant, *typesfakes.FakeMediaTrack) {
	p := rtc.NewMockParticipant(identity, types.CurrentProtocol, false, true, room.LocalParticipantListener())
	require.NoError(t, room.Join(p, nil, &rtc.ParticipantOptions{}, nil))
	p.StateReturns(livekit.ParticipantInfo_ACTIVE)
	p.IsReadyReturns(true)
	p.HasPermissionReturns(true)
	track := rtc.NewMockTrack(livekit.TrackType_AUDIO, "mic")
	track.IsOpenReturns(true)
	p.GetPublishedTracksReturns([]types.MediaTrack{track})
	p.GetPublishedTrackReturns(track)
	return p, track
}

// removeViaBus sends RemoveParticipant over psrpc, the way RoomService routes it, and reports whether a server answered.
func (e *forwardTestEnv) removeViaBus(t *testing.T, room, identity string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := e.client.RemoveParticipant(
		ctx,
		rpc.FormatParticipantTopic(livekit.RoomName(room), livekit.ParticipantIdentity(identity)),
		&livekit.RoomParticipantIdentity{Room: room, Identity: identity},
		psrpc.WithRequestTimeout(500*time.Millisecond),
	)
	return err
}

func TestRoomManagerForwardParticipant(t *testing.T) {
	ctx := context.Background()

	t.Run("forward into an existing local room and manage it through the destination", func(t *testing.T) {
		env := newForwardTestEnv(t)
		src := env.newRoom(t, "radio")
		dest := env.newRoom(t, "channel")
		bc, track := env.newSource(t, src, "bc:alice")

		_, err := env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "channel"})
		require.NoError(t, err)
		require.Equal(t, bc, dest.GetForwardedParticipant("bc:alice"))

		// idempotent
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "channel"})
		require.NoError(t, err)

		// stored for ListParticipants/GetParticipant via the store
		stored, err := env.rm.roomStore.LoadParticipant(ctx, "channel", "bc:alice")
		require.NoError(t, err)
		require.Contains(t, stored.KindDetails, livekit.ParticipantInfo_FORWARDED)
		require.Equal(t, string(bc.ID()), stored.Sid)

		// and via the psrpc GetParticipant/ListParticipants handlers
		pi, err := env.rm.GetParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "channel", Identity: "bc:alice"})
		require.NoError(t, err)
		require.Contains(t, pi.KindDetails, livekit.ParticipantInfo_FORWARDED)
		list, err := env.rm.ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: "channel"})
		require.NoError(t, err)
		require.Len(t, list.Participants, 1)
		require.Equal(t, "bc:alice", list.Participants[0].Identity)

		// updates are refused, mutes hit the source track
		_, err = env.rm.UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{Room: "channel", Identity: "bc:alice", Metadata: "x"})
		require.ErrorIs(t, err, ErrForwardedParticipantReadOnly)
		_, err = env.rm.UpdateSubscriptions(ctx, &livekit.UpdateSubscriptionsRequest{Room: "channel", Identity: "bc:alice"})
		require.ErrorIs(t, err, ErrForwardedParticipantReadOnly)
		_, err = env.rm.MutePublishedTrack(ctx, &livekit.MuteRoomTrackRequest{Room: "channel", Identity: "bc:alice", TrackSid: string(track.ID()), Muted: true})
		require.NoError(t, err)
		require.Equal(t, 1, bc.SetTrackMutedCallCount())

		// the participant topic for (channel, bc:alice) is served by this node, and stops being served once unforwarded
		require.NoError(t, env.removeViaBus(t, "channel", "bc:alice"))
		require.Nil(t, dest.GetForwardedParticipant("bc:alice"))
		_, err = env.rm.roomStore.LoadParticipant(ctx, "channel", "bc:alice")
		require.ErrorIs(t, err, ErrParticipantNotFound)
		require.Error(t, env.removeViaBus(t, "channel", "bc:alice"))

		// the source is untouched
		require.Equal(t, bc, src.GetParticipant("bc:alice"))
		_, err = env.rm.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "channel", Identity: "bc:alice"})
		require.ErrorIs(t, err, ErrParticipantNotFound)
	})

	t.Run("destination is created locally when missing and released when the source leaves", func(t *testing.T) {
		env := newForwardTestEnv(t)
		src := env.newRoomWithInternal(t, "radio", &livekit.RoomInternal{Tags: map[string]string{RoomAPIKeyTag: "key_radio"}})
		bc, _ := env.newSource(t, src, "bc:alice")

		_, err := env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "channel"})
		require.NoError(t, err)

		dest := env.rm.GetRoom(ctx, "channel")
		require.NotNil(t, dest)
		require.Equal(t, bc, dest.GetForwardedParticipant("bc:alice"))
		require.Equal(t, 1, env.router.SetNodeForRoomCallCount())
		_, roomName, nodeID := env.router.SetNodeForRoomArgsForCall(0)
		require.Equal(t, livekit.RoomName("channel"), roomName)
		require.Equal(t, forwardTestNodeID, nodeID)

		// the new room inherits the source room's API key tag so its webhooks route the same way
		require.NotNil(t, env.allocator.lastReq)
		require.Equal(t, "channel", env.allocator.lastReq.Name)
		require.Equal(t, "key_radio", env.allocator.lastReq.GetTags()[RoomAPIKeyTag])

		// a forwarded publisher alone keeps the destination open
		dest.CloseIfEmpty()
		require.False(t, dest.IsClosed())

		src.RemoveParticipant("bc:alice", bc.ID(), types.ParticipantCloseReasonClientRequestLeave)
		require.Nil(t, dest.GetForwardedParticipant("bc:alice"))
		_, err = env.rm.roomStore.LoadParticipant(ctx, "channel", "bc:alice")
		require.ErrorIs(t, err, ErrParticipantNotFound)
		require.Error(t, env.removeViaBus(t, "channel", "bc:alice"))
	})

	t.Run("errors", func(t *testing.T) {
		env := newForwardTestEnv(t)
		src := env.newRoom(t, "radio")
		env.newSource(t, src, "bc:alice")

		_, err := env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "nobody", DestinationRoom: "channel"})
		require.ErrorIs(t, err, ErrParticipantNotFound)
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "elsewhere", Identity: "bc:alice", DestinationRoom: "channel"})
		require.ErrorIs(t, err, ErrRoomNotFound)
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "radio"})
		require.ErrorIs(t, err, ErrDestinationSameAsSourceRoom)

		// identity in use in the destination
		dest := env.newRoom(t, "channel")
		env.newSource(t, dest, "bc:alice")
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "channel"})
		require.ErrorIs(t, err, ErrForwardIdentityInUse)

		// destination hosted on another live node
		env.router.GetNodeForRoomReturns(&livekit.Node{Id: "ND_other", State: livekit.NodeState_SERVING, Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}, nil)
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "remote"})
		require.ErrorIs(t, err, ErrForwardCrossNode)
		require.Nil(t, env.rm.GetRoom(ctx, "remote"))

		// a stale assignment to a dead node is taken over
		env.router.GetNodeForRoomReturns(&livekit.Node{Id: "ND_dead", Stats: &livekit.NodeStats{UpdatedAt: 1}}, nil)
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "orphaned"})
		require.NoError(t, err)
		require.NotNil(t, env.rm.GetRoom(ctx, "orphaned"))

		// hidden participants cannot be forwarded
		hidden := rtc.NewMockParticipant("ghost", types.CurrentProtocol, true, false, src.LocalParticipantListener())
		require.NoError(t, src.Join(hidden, nil, &rtc.ParticipantOptions{}, nil))
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "ghost", DestinationRoom: "channel"})
		require.ErrorIs(t, err, ErrForwardHiddenParticipant)

		// auto-create disabled and destination unknown
		env.router.GetNodeForRoomReturns(nil, routing.ErrNotFound)
		env.allocator.validateErr = ErrRoomNotFound
		_, err = env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "bc:alice", DestinationRoom: "missing"})
		require.ErrorIs(t, err, ErrRoomNotFound)
	})
}

func TestForwardCloser(t *testing.T) {
	t.Run("set then close", func(t *testing.T) {
		c := &forwardCloser{}
		n := 0
		c.set(func() { n++ })
		c.close()
		c.close()
		require.Equal(t, 1, n)
	})

	t.Run("close then set runs immediately", func(t *testing.T) {
		c := &forwardCloser{}
		n := 0
		c.close()
		c.set(func() { n++ })
		require.Equal(t, 1, n)
	})
}
