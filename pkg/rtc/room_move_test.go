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

package rtc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/audio"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
)

// newMoveTestRoom is a room with its own name and SID, so that two of them can be told apart.
func newMoveTestRoom(t *testing.T, name string, ts telemetry.TelemetryService) *Room {
	if ts == nil {
		ts = &telemetryfakes.FakeTelemetryService{}
	}
	room := NewRoom(
		&livekit.Room{Name: name, Sid: "RM_" + name},
		nil,
		WebRTCConfig{},
		config.RoomConfig{EmptyTimeout: 5 * 60, DepartureTimeout: 1},
		&sfu.AudioConfig{AudioLevelConfig: audio.AudioLevelConfig{UpdateInterval: audioUpdateInterval}},
		&livekit.ServerInfo{NodeId: "testnode", Region: "testregion"},
		ts,
		nil, nil, nil,
	)
	t.Cleanup(func() { room.Close(types.RoomCloseReasonUnknown) })
	return room
}

// newMovable joins an active publisher with one open audio track. The fake rebinds itself on MoveToRoom
// the way ParticipantImpl does: new SID, telemetry listener of the destination.
func newMovable(t *testing.T, room *Room, identity livekit.ParticipantIdentity, autoSubscribe bool) (*typesfakes.FakeLocalParticipant, *typesfakes.FakeMediaTrack) {
	p := NewMockParticipant(identity, types.CurrentProtocol, false, true, room.LocalParticipantListener())
	require.NoError(t, room.Join(p, nil, &ParticipantOptions{AutoSubscribe: autoSubscribe}, iceServersForRoom))
	p.StateReturns(livekit.ParticipantInfo_ACTIVE)
	p.IsReadyReturns(true)
	p.HasPermissionReturns(true)
	p.ToProtoReturns(&livekit.ParticipantInfo{
		Sid:         string(p.ID()),
		Identity:    string(identity),
		State:       livekit.ParticipantInfo_ACTIVE,
		IsPublisher: true,
	})
	track := newOpenMockTrack(p)
	p.GetPublishedTracksReturns([]types.MediaTrack{track})
	p.MoveToRoomCalls(func(params types.MoveToRoomParams) {
		p.IDReturns(params.ParticipantID)
		track.PublisherIDReturns(params.ParticipantID)
		pi := utils.CloneProto(p.ToProto())
		pi.Sid = string(params.ParticipantID)
		p.ToProtoReturns(pi)
		p.GetTelemetryListenerReturns(params.TelemetryListener)
	})
	return p, track
}

func TestMoveParticipant(t *testing.T) {
	t.Run("keeps its tracks, source sees a leave, destination sees a join", func(t *testing.T) {
		src := newMoveTestRoom(t, "src", nil)
		destTelemetry := &telemetryfakes.FakeTelemetryService{}
		dest := newMoveTestRoom(t, "dest", destTelemetry)
		other := newMoveTestRoom(t, "other", nil)
		radio := newMoveTestRoom(t, "radio", nil)

		witness, _ := newMovable(t, src, "witness", true)
		mover, moverTrack := newMovable(t, src, "mover", true)
		resident, residentTrack := newMovable(t, dest, "resident", true)
		bc, bcTrack := newMovable(t, radio, "bc", false)
		require.NoError(t, dest.AddForwardedParticipant(bc, radio, ForwardHooks{}))
		probe := &forwardProbe{}
		require.NoError(t, other.AddForwardedParticipant(mover, src, probe.hooks()))

		oldID := mover.ID()
		newID := livekit.ParticipantID("PA_moved")
		helper := &typesfakes.FakeLocalParticipantHelper{}
		require.NoError(t, src.MoveParticipant(dest, mover, MoveParticipantParams{ParticipantID: newID, Token: "tok", Helper: helper}))

		// rebound by MoveToRoom with the listeners of the destination
		require.Equal(t, 1, mover.MoveToRoomCallCount())
		params := mover.MoveToRoomArgsForCall(0)
		require.Equal(t, dest.Name(), params.RoomName)
		require.Equal(t, newID, params.ParticipantID)
		require.Equal(t, dest.LocalParticipantListener(), params.Listener)
		require.Equal(t, dest.ParticipantTelemetryListener(), params.TelemetryListener)
		require.Equal(t, helper, params.Helper)
		require.Equal(t, 0, mover.CloseCallCount())

		// gone from the source, which saw a leave; forwards out of the source ended
		require.Nil(t, src.GetParticipant("mover"))
		require.Nil(t, src.trackManager.GetTrackInfo(moverTrack.ID()))
		left := lastUpdateFor(witness, "mover")
		require.NotNil(t, left)
		require.Equal(t, string(oldID), left.Sid)
		require.Equal(t, livekit.ParticipantInfo_DISCONNECTED, left.State)
		require.Equal(t, MovedDisconnectReason, left.DisconnectReason)
		require.Nil(t, other.GetForwardedParticipant("mover"))
		require.True(t, probe.ended.Load())

		// in the destination with its track, under the new SID
		require.Equal(t, mover, dest.GetParticipant("mover"))
		require.NotNil(t, dest.trackManager.GetTrackInfo(moverTrack.ID()))
		res := dest.ResolveMediaTrackForSubscriber(resident, moverTrack.ID())
		require.Equal(t, moverTrack, res.Track)
		require.Equal(t, newID, res.PublisherID)

		// the client gets the JoinResponse equivalent, including the forwarded publisher of the destination
		require.Equal(t, 1, mover.SendRoomMovedResponseCallCount())
		moved := mover.SendRoomMovedResponseArgsForCall(0)
		require.Equal(t, string(dest.ID()), moved.Room.Sid)
		require.Equal(t, "tok", moved.Token)
		require.Equal(t, string(newID), moved.Participant.Sid)
		others := make(map[string]*livekit.ParticipantInfo)
		for _, pi := range moved.OtherParticipants {
			others[pi.Identity] = pi
		}
		require.Len(t, others, 2)
		require.Equal(t, string(resident.ID()), others["resident"].Sid)
		requireForwardedInfo(t, others["bc"], bc)

		// residents see it join and subscribe to its track; it subscribes to theirs and to forwarded ones
		joined := lastUpdateFor(resident, "mover")
		require.NotNil(t, joined)
		require.Equal(t, string(newID), joined.Sid)
		require.Equal(t, livekit.ParticipantInfo_ACTIVE, joined.State)
		require.Contains(t, subscribedTrackIDs(resident), moverTrack.ID())
		ids := subscribedTrackIDs(mover)
		require.Contains(t, ids, residentTrack.ID())
		require.Contains(t, ids, bcTrack.ID())

		// telemetry: a new session in the destination, the track is published again there
		require.Equal(t, 1, destTelemetry.ParticipantJoinedCallCount())
		_, room, pi, _, _, sendEvent, _ := destTelemetry.ParticipantJoinedArgsForCall(0)
		require.Equal(t, string(dest.ID()), room.Sid)
		require.Equal(t, string(newID), pi.Sid)
		require.True(t, sendEvent)
		require.Equal(t, 1, destTelemetry.ParticipantActiveCallCount())
		_, _, _, _, isMigration, _, _ := destTelemetry.ParticipantActiveArgsForCall(0)
		require.False(t, isMigration)
		require.Equal(t, 1, destTelemetry.TrackPublishedCallCount())
		_, room, pID, identity, ti, sendEvent := destTelemetry.TrackPublishedArgsForCall(0)
		require.Equal(t, string(dest.ID()), room.Sid)
		require.Equal(t, newID, pID)
		require.Equal(t, livekit.ParticipantIdentity("mover"), identity)
		require.Equal(t, moverTrack.ToProto().Name, ti.Name)
		require.True(t, sendEvent)

		require.Len(t, src.GetParticipants(), 1)
		require.Len(t, dest.GetParticipants(), 2)
	})

	t.Run("hidden participant moves without updates", func(t *testing.T) {
		src := newMoveTestRoom(t, "src", nil)
		dest := newMoveTestRoom(t, "dest", nil)
		witness, _ := newMovable(t, src, "witness", true)
		resident, _ := newMovable(t, dest, "resident", true)
		ghost := NewMockParticipant("ghost", types.CurrentProtocol, true, false, src.LocalParticipantListener())
		require.NoError(t, src.Join(ghost, nil, &ParticipantOptions{AutoSubscribe: true}, iceServersForRoom))
		ghost.StateReturns(livekit.ParticipantInfo_ACTIVE)

		require.NoError(t, src.MoveParticipant(dest, ghost, MoveParticipantParams{ParticipantID: "PA_ghost"}))

		require.Equal(t, ghost, dest.GetParticipant("ghost"))
		require.Nil(t, src.GetParticipant("ghost"))
		require.Nil(t, lastUpdateFor(witness, "ghost"))
		require.Nil(t, lastUpdateFor(resident, "ghost"))
		moved := ghost.SendRoomMovedResponseArgsForCall(0)
		require.Len(t, moved.OtherParticipants, 1)
		require.Equal(t, "resident", moved.OtherParticipants[0].Identity)
	})

	t.Run("errors leave the participant in the source", func(t *testing.T) {
		src := newMoveTestRoom(t, "src", nil)
		dest := newMoveTestRoom(t, "dest", nil)
		mover, _ := newMovable(t, src, "mover", true)
		params := MoveParticipantParams{ParticipantID: "PA_moved"}

		require.ErrorIs(t, src.MoveParticipant(src, mover, params), ErrMoveSameRoom)

		// identity in use in the destination, joined or forwarded
		twin, _ := newMovable(t, dest, "mover", true)
		require.ErrorIs(t, src.MoveParticipant(dest, mover, params), ErrAlreadyJoined)
		dest.RemoveParticipant("mover", twin.ID(), types.ParticipantCloseReasonClientRequestLeave)
		require.NoError(t, dest.AddForwardedParticipant(mover, src, ForwardHooks{}))
		require.ErrorIs(t, src.MoveParticipant(dest, mover, params), ErrAlreadyJoined)
		require.True(t, dest.RemoveForwardedParticipant("mover"))

		// not a participant of the source
		stranger := NewMockParticipant("stranger", types.CurrentProtocol, false, true, nil)
		require.ErrorIs(t, src.MoveParticipant(dest, stranger, params), ErrMoveSourceGone)

		// room limits
		newMovable(t, dest, "resident", true)
		dest.protoRoom.MaxParticipants = 1
		require.ErrorIs(t, src.MoveParticipant(dest, mover, params), ErrMaxParticipantsExceeded)
		dest.protoRoom.MaxParticipants = 0

		// closed destination
		closed := newMoveTestRoom(t, "closed", nil)
		closed.Close(types.RoomCloseReasonUnknown)
		require.ErrorIs(t, src.MoveParticipant(closed, mover, params), ErrRoomClosed)

		require.Equal(t, mover, src.GetParticipant("mover"))
		require.Equal(t, 0, mover.MoveToRoomCallCount())
		require.Equal(t, 0, mover.SendRoomMovedResponseCallCount())
	})

	t.Run("attach failure closes the participant instead of stranding it", func(t *testing.T) {
		src := newMoveTestRoom(t, "src", nil)
		dest := newMoveTestRoom(t, "dest", nil)
		mover, _ := newMovable(t, src, "mover", true)
		mover.SendRoomMovedResponseReturns(errors.New("sink closed"))

		err := src.MoveParticipant(dest, mover, MoveParticipantParams{ParticipantID: "PA_moved"})
		require.EqualError(t, err, "sink closed")
		require.Nil(t, src.GetParticipant("mover"))
		require.Equal(t, 1, mover.CloseCallCount())
		_, reason, _ := mover.CloseArgsForCall(0)
		require.Equal(t, types.ParticipantCloseReasonMoveFailed, reason)
	})
}
