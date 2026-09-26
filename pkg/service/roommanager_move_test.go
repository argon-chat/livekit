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

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/must"
	"github.com/livekit/psrpc"

	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
)

// newMovable joins a fake participant that rebinds itself on MoveToRoom the way ParticipantImpl does: the
// close callbacks of the session run, then SID, proto and grants room change. Close runs the callbacks too.
func (e *forwardTestEnv) newMovable(t *testing.T, room *rtc.Room, identity livekit.ParticipantIdentity, apiKey string, expiresAt time.Time) (*typesfakes.FakeLocalParticipant, *typesfakes.FakeMediaTrack) {
	p, track := e.newSource(t, room, identity)
	p.APIKeyReturns(apiKey)
	p.TokenExpiresAtReturns(expiresAt)
	canPublish := false
	p.ClaimGrantsReturns(&auth.ClaimGrants{
		Identity:   string(identity),
		Name:       "Alice",
		Metadata:   "meta",
		Attributes: map[string]string{"k": "v"},
		Video:      &auth.VideoGrant{Room: string(room.Name()), RoomJoin: true, CanPublish: &canPublish},
	})

	closers := make(map[string]func(types.LocalParticipant))
	p.AddOnCloseCalls(func(key string, cb func(types.LocalParticipant)) { closers[key] = cb })
	runClosers := func() {
		cbs := closers
		closers = make(map[string]func(types.LocalParticipant))
		for _, cb := range cbs {
			cb(p)
		}
	}
	p.CloseCalls(func(bool, types.ParticipantCloseReason, bool) error {
		runClosers()
		return nil
	})
	p.MoveToRoomCalls(func(params types.MoveToRoomParams) {
		runClosers()
		p.IDReturns(params.ParticipantID)
		pi := utils.CloneProto(p.ToProto())
		pi.Sid = string(params.ParticipantID)
		p.ToProtoReturns(pi)
		grants := p.ClaimGrants().Clone()
		grants.Video.Room = string(params.RoomName)
		p.ClaimGrantsReturns(grants)
	})
	return p, track
}

// bindSession mirrors StartSession: the participant topic for (room, identity) is served and the store
// holds the record until the close callback of the session runs.
func (e *forwardTestEnv) bindSession(t *testing.T, room *rtc.Room, p types.LocalParticipant) {
	topic := rpc.FormatParticipantTopic(room.Name(), p.Identity())
	server := must.Get(rpc.NewTypedParticipantServer(e.rm, e.rm.bus))
	kill := e.rm.participantServers.Replace(topic, server)
	require.NoError(t, server.RegisterAllParticipantTopics(topic))
	require.NoError(t, e.rm.roomStore.StoreParticipant(context.Background(), room.Name(), p.ToProto()))
	p.AddOnClose(types.ParticipantCloseKeyNormal, func(p types.LocalParticipant) {
		kill()
		_ = e.rm.roomStore.DeleteParticipant(context.Background(), room.Name(), p.Identity())
	})
}

// probeViaBus sends an empty UpdateParticipant over psrpc and reports whether a server for (room, identity) answered.
func (e *forwardTestEnv) probeViaBus(t *testing.T, room, identity string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := e.client.UpdateParticipant(
		ctx,
		rpc.FormatParticipantTopic(livekit.RoomName(room), livekit.ParticipantIdentity(identity)),
		&livekit.UpdateParticipantRequest{Room: room, Identity: identity},
		psrpc.WithRequestTimeout(500*time.Millisecond),
	)
	return err
}

func TestRoomManagerMoveParticipant(t *testing.T) {
	ctx := context.Background()

	t.Run("move between two local rooms", func(t *testing.T) {
		env := newForwardTestEnv(t)
		env.rm.config.Keys = map[string]string{"key_radio": "secret_radio", "key_other": "secret_other"}
		src := env.newRoom(t, "radio")
		dest := env.newRoom(t, "channel")
		other := env.newRoom(t, "other")
		expiresAt := time.Now().Add(time.Hour)
		alice, _ := env.newMovable(t, src, "alice", "key_radio", expiresAt)
		env.bindSession(t, src, alice)
		env.newSource(t, dest, "bob")
		_, err := env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "alice", DestinationRoom: "other"})
		require.NoError(t, err)
		require.NoError(t, env.probeViaBus(t, "radio", "alice"))

		oldID := alice.ID()
		_, err = env.rm.MoveParticipant(ctx, &livekit.MoveParticipantRequest{Room: "radio", Identity: "alice", DestinationRoom: "channel"})
		require.NoError(t, err)
		require.NotEqual(t, oldID, alice.ID())

		// rooms: out of the source and its forwards, in the destination with a helper bound to it
		require.Nil(t, src.GetParticipant("alice"))
		require.Equal(t, alice, dest.GetParticipant("alice"))
		require.Nil(t, other.GetForwardedParticipant("alice"))
		require.Equal(t, 1, alice.MoveToRoomCallCount())
		require.Equal(t, dest, alice.MoveToRoomArgsForCall(0).Helper.(*roomManagerParticipantHelper).room)

		// store: the record moved rooms
		stored, err := env.rm.roomStore.LoadParticipant(ctx, "channel", "alice")
		require.NoError(t, err)
		require.Equal(t, string(alice.ID()), stored.Sid)
		_, err = env.rm.roomStore.LoadParticipant(ctx, "radio", "alice")
		require.ErrorIs(t, err, ErrParticipantNotFound)
		_, err = env.rm.roomStore.LoadParticipant(ctx, "other", "alice")
		require.ErrorIs(t, err, ErrParticipantNotFound)

		// topics: served for the destination, no longer for the source
		require.NoError(t, env.probeViaBus(t, "channel", "alice"))
		require.Error(t, env.probeViaBus(t, "radio", "alice"))

		// the client gets the destination and a reconnect token signed with the key it was admitted with
		require.Equal(t, 1, alice.SendRoomMovedResponseCallCount())
		moved := alice.SendRoomMovedResponseArgsForCall(0)
		require.Equal(t, "channel", moved.Room.Name)
		require.Equal(t, string(alice.ID()), moved.Participant.Sid)
		require.Len(t, moved.OtherParticipants, 1)
		require.Equal(t, "bob", moved.OtherParticipants[0].Identity)

		v, err := auth.ParseAPIToken(moved.Token)
		require.NoError(t, err)
		require.Equal(t, "key_radio", v.APIKey())
		kp := auth.NewFileBasedKeyProviderFromMap(env.rm.config.Keys)
		claims, grants, err := v.Verify(kp.GetSecret(v.APIKey()))
		require.NoError(t, err)
		require.Equal(t, "alice", grants.Identity)
		require.Equal(t, "Alice", grants.Name)
		require.Equal(t, "meta", grants.Metadata)
		require.Equal(t, map[string]string{"k": "v"}, grants.Attributes)
		require.Equal(t, livekit.ParticipantInfo_STANDARD, grants.GetParticipantKind())
		require.Equal(t, "channel", grants.Video.Room)
		require.Empty(t, grants.Video.DestinationRoom)
		require.True(t, grants.Video.RoomJoin)
		require.False(t, grants.Video.GetCanPublish())
		require.WithinDuration(t, expiresAt, claims.ExpiresAt.Time, 5*time.Second)
		_, _, err = v.Verify("secret_other")
		require.Error(t, err)

		// leaving the destination cleans up there
		require.NoError(t, env.removeViaBus(t, "channel", "alice"))
		require.Nil(t, dest.GetParticipant("alice"))
		_, err = env.rm.roomStore.LoadParticipant(ctx, "channel", "alice")
		require.ErrorIs(t, err, ErrParticipantNotFound)
		require.Error(t, env.probeViaBus(t, "channel", "alice"))
	})

	t.Run("destination is created locally, signing key falls back to the source room tag", func(t *testing.T) {
		env := newForwardTestEnv(t)
		env.rm.config.Keys = map[string]string{"key_radio": "secret_radio"}
		src := env.newRoomWithInternal(t, "radio", &livekit.RoomInternal{Tags: map[string]string{RoomAPIKeyTag: "key_radio"}})
		alice, _ := env.newMovable(t, src, "alice", "", time.Time{})

		_, err := env.rm.MoveParticipant(ctx, &livekit.MoveParticipantRequest{Room: "radio", Identity: "alice", DestinationRoom: "channel"})
		require.NoError(t, err)

		dest := env.rm.GetRoom(ctx, "channel")
		require.NotNil(t, dest)
		require.Equal(t, alice, dest.GetParticipant("alice"))
		require.Equal(t, 1, env.router.SetNodeForRoomCallCount())
		require.Equal(t, "key_radio", env.allocator.lastReq.GetTags()[RoomAPIKeyTag])

		v, err := auth.ParseAPIToken(alice.SendRoomMovedResponseArgsForCall(0).Token)
		require.NoError(t, err)
		require.Equal(t, "key_radio", v.APIKey())
		claims, _, err := v.Verify("secret_radio")
		require.NoError(t, err)
		// no expiry known: the refresh token default
		require.WithinDuration(t, time.Now().Add(tokenDefaultTTL), claims.ExpiresAt.Time, 5*time.Second)
	})

	t.Run("errors", func(t *testing.T) {
		env := newForwardTestEnv(t)
		env.rm.config.Keys = map[string]string{"key_radio": "secret_radio"}
		src := env.newRoom(t, "radio")
		alice, _ := env.newMovable(t, src, "alice", "key_radio", time.Time{})
		move := func(room, identity, dest string) error {
			_, err := env.rm.MoveParticipant(ctx, &livekit.MoveParticipantRequest{Room: room, Identity: identity, DestinationRoom: dest})
			return err
		}

		require.ErrorIs(t, move("radio", "nobody", "channel"), ErrParticipantNotFound)
		require.ErrorIs(t, move("elsewhere", "alice", "channel"), ErrRoomNotFound)
		require.ErrorIs(t, move("radio", "alice", "radio"), ErrDestinationSameAsSourceRoom)
		require.ErrorIs(t, move("radio", "alice", ""), ErrNoRoomName)

		// a forwarded entry cannot be moved from its destination and blocks a move of its identity there
		dest := env.newRoom(t, "channel")
		_, err := env.rm.ForwardParticipant(ctx, &livekit.ForwardParticipantRequest{Room: "radio", Identity: "alice", DestinationRoom: "channel"})
		require.NoError(t, err)
		require.ErrorIs(t, move("channel", "alice", "other"), ErrForwardedParticipantReadOnly)
		require.ErrorIs(t, move("radio", "alice", "channel"), ErrMoveIdentityInUse)
		_, err = env.rm.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "channel", Identity: "alice"})
		require.NoError(t, err)
		env.newSource(t, dest, "alice")
		require.ErrorIs(t, move("radio", "alice", "channel"), ErrMoveIdentityInUse)

		// destination hosted on another live node
		env.router.GetNodeForRoomReturns(&livekit.Node{Id: "ND_other", State: livekit.NodeState_SERVING, Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}, nil)
		require.ErrorIs(t, move("radio", "alice", "remote"), ErrMoveCrossNode)
		require.Nil(t, env.rm.GetRoom(ctx, "remote"))
		env.router.GetNodeForRoomReturns(nil, routing.ErrNotFound)

		// clients that cannot be moved
		alice.SupportsMovingReturns(rtc.ErrMoveOldClientVersion)
		require.ErrorIs(t, move("radio", "alice", "new"), rtc.ErrMoveOldClientVersion)
		alice.SupportsMovingReturns(nil)

		// no usable signing key
		alice.APIKeyReturns("key_unknown")
		require.ErrorIs(t, move("radio", "alice", "new"), ErrMoveNoSigningKey)
		alice.APIKeyReturns("")
		require.ErrorIs(t, move("radio", "alice", "new"), ErrMoveNoSigningKey)
		alice.APIKeyReturns("key_radio")

		// auto-create disabled and destination unknown
		env.allocator.validateErr = ErrRoomNotFound
		require.ErrorIs(t, move("radio", "alice", "missing"), ErrRoomNotFound)

		require.Nil(t, env.rm.GetRoom(ctx, "new"))
		require.Equal(t, alice, src.GetParticipant("alice"))
		require.Equal(t, 0, alice.MoveToRoomCallCount())
	})
}
