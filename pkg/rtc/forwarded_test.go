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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
)

// newForwardSource joins a publisher with one open audio track into src.
func newForwardSource(t *testing.T, src *Room, identity livekit.ParticipantIdentity) (*typesfakes.FakeLocalParticipant, *typesfakes.FakeMediaTrack) {
	p := NewMockParticipant(identity, types.CurrentProtocol, false, true, src.LocalParticipantListener())
	require.NoError(t, src.Join(p, nil, &ParticipantOptions{AutoSubscribe: false}, iceServersForRoom))
	p.StateReturns(livekit.ParticipantInfo_ACTIVE)
	p.IsReadyReturns(true)
	p.HasPermissionReturns(true)
	track := newOpenMockTrack(p)
	p.GetPublishedTracksReturns([]types.MediaTrack{track})
	return p, track
}

func newOpenMockTrack(pub types.Participant) *typesfakes.FakeMediaTrack {
	track := NewMockTrack(livekit.TrackType_AUDIO, "mic")
	track.IsOpenReturns(true)
	track.PublisherIDReturns(pub.ID())
	track.PublisherIdentityReturns(pub.Identity())
	return track
}

type forwardProbe struct {
	updates atomic.Int32
	ended   atomic.Bool
	last    atomic.Pointer[livekit.ParticipantInfo]
}

func (fp *forwardProbe) hooks() ForwardHooks {
	return ForwardHooks{
		OnUpdate: func(pi *livekit.ParticipantInfo) {
			fp.updates.Inc()
			fp.last.Store(pi)
		},
		OnEnd: func() { fp.ended.Store(true) },
	}
}

func lastUpdateFor(p *typesfakes.FakeLocalParticipant, identity livekit.ParticipantIdentity) *livekit.ParticipantInfo {
	for i := p.SendParticipantUpdateCallCount() - 1; i >= 0; i-- {
		for _, pi := range p.SendParticipantUpdateArgsForCall(i) {
			if pi.Identity == string(identity) {
				return pi
			}
		}
	}
	return nil
}

func subscribedTrackIDs(p *typesfakes.FakeLocalParticipant) []livekit.TrackID {
	ids := make([]livekit.TrackID, 0, p.SubscribeToTrackCallCount())
	for i := range p.SubscribeToTrackCallCount() {
		id, _ := p.SubscribeToTrackArgsForCall(i)
		ids = append(ids, id)
	}
	return ids
}

func requireForwardedInfo(t *testing.T, pi *livekit.ParticipantInfo, src types.Participant) {
	require.NotNil(t, pi)
	require.Equal(t, string(src.ID()), pi.Sid)
	require.Equal(t, string(src.Identity()), pi.Identity)
	require.Contains(t, pi.KindDetails, livekit.ParticipantInfo_FORWARDED)
	require.Equal(t, livekit.ParticipantInfo_ACTIVE, pi.State)
}

func TestForwardedParticipant(t *testing.T) {
	t.Run("forward adds tracks and subscribes auto-subscribe participants", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 2})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, track := newForwardSource(t, src, "bc")

		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))

		// source proto is never mutated
		require.NotContains(t, bc.ToProto().KindDetails, livekit.ParticipantInfo_FORWARDED)

		require.Equal(t, bc, dest.GetForwardedParticipant("bc"))
		require.Len(t, dest.GetForwardedParticipantInfos(), 1)
		requireForwardedInfo(t, dest.GetForwardedParticipantInfos()[0], bc)
		requireForwardedInfo(t, probe.last.Load(), bc)
		require.EqualValues(t, 1, probe.updates.Load())

		require.NotNil(t, dest.trackManager.GetTrackInfo(track.ID()))
		for _, op := range dest.GetParticipants() {
			fp := op.(*typesfakes.FakeLocalParticipant)
			require.Contains(t, subscribedTrackIDs(fp), track.ID())
			requireForwardedInfo(t, lastUpdateFor(fp, "bc"), bc)
		}

		sub := dest.GetParticipants()[0]
		res := dest.ResolveMediaTrackForSubscriber(sub, track.ID())
		require.Equal(t, track, res.Track)
		require.True(t, res.HasPermission)
		require.Equal(t, bc.ID(), res.PublisherID)
		require.Equal(t, bc.Identity(), res.PublisherIdentity)

		// the destination never counts the forwarded publisher
		require.EqualValues(t, 2, dest.updateProto().NumParticipants)
		require.False(t, probe.ended.Load())
	})

	t.Run("participant joining later sees the forwarded participant and subscribes", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, track := newForwardSource(t, src, "bc")
		require.NoError(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}))

		lpl := dest.LocalParticipantListener()
		pNew := NewMockParticipant("new", types.CurrentProtocol, false, false, lpl)
		require.NoError(t, dest.Join(pNew, &routingfakes.FakeMessageSource{}, &ParticipantOptions{AutoSubscribe: true}, iceServersForRoom))

		join := pNew.SendJoinResponseArgsForCall(0)
		require.Len(t, join.OtherParticipants, 2)
		var forwarded *livekit.ParticipantInfo
		for _, pi := range join.OtherParticipants {
			if pi.Identity == "bc" {
				forwarded = pi
			}
		}
		requireForwardedInfo(t, forwarded, bc)

		pNew.StateReturns(livekit.ParticipantInfo_ACTIVE)
		lpl.OnStateChange(pNew)
		require.Contains(t, subscribedTrackIDs(pNew), track.ID())

		// resume also carries the forwarded participant
		require.NoError(t, dest.ResumeParticipant(pNew, &routingfakes.FakeMessageSource{}, nil, nil, iceServersForRoom, livekit.ReconnectReason_RR_UNKNOWN))
		requireForwardedInfo(t, lastUpdateFor(pNew, "bc"), bc)
	})

	t.Run("source publish and unpublish propagate to the destination", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))

		p0 := dest.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
		updatesBefore := p0.SendParticipantUpdateCallCount()
		hookUpdatesBefore := probe.updates.Load()

		track2 := newOpenMockTrack(bc)
		src.LocalParticipantListener().OnTrackPublished(bc, track2)
		require.NotNil(t, dest.trackManager.GetTrackInfo(track2.ID()))
		require.Contains(t, subscribedTrackIDs(p0), track2.ID())
		require.Equal(t, updatesBefore+1, p0.SendParticipantUpdateCallCount())
		require.Equal(t, hookUpdatesBefore+1, probe.updates.Load())

		src.LocalParticipantListener().OnTrackUnpublished(bc, track2)
		require.Nil(t, dest.trackManager.GetTrackInfo(track2.ID()))
		require.Equal(t, updatesBefore+2, p0.SendParticipantUpdateCallCount())
		require.Nil(t, dest.ResolveMediaTrackForSubscriber(p0, track2.ID()).Track)
	})

	t.Run("source mute and metadata updates propagate to the destination", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		require.NoError(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}))

		p0 := dest.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
		before := p0.SendParticipantUpdateCallCount()

		bc.SetTrackMuted(&livekit.MuteTrackRequest{Muted: true}, true)
		require.Equal(t, before+1, p0.SendParticipantUpdateCallCount())

		bc.SetMetadata("radio")
		require.Equal(t, before+2, p0.SendParticipantUpdateCallCount())
		requireForwardedInfo(t, lastUpdateFor(p0, "bc"), bc)
	})

	t.Run("source leaving ends the forward with a disconnect update", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, track := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))
		require.Len(t, src.forwardTargetsOf(bc.ID()), 1)

		src.RemoveParticipant(bc.Identity(), bc.ID(), types.ParticipantCloseReasonClientRequestLeave)

		require.True(t, probe.ended.Load())
		require.Nil(t, dest.GetForwardedParticipant("bc"))
		require.Empty(t, dest.GetForwardedParticipantInfos())
		require.Empty(t, src.forwardTargetsOf(bc.ID()))
		require.Nil(t, dest.trackManager.GetTrackInfo(track.ID()))

		p0 := dest.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
		last := lastUpdateFor(p0, "bc")
		require.NotNil(t, last)
		require.Equal(t, livekit.ParticipantInfo_DISCONNECTED, last.State)
		require.Contains(t, last.KindDetails, livekit.ParticipantInfo_FORWARDED)

		// removing again is a no-op
		require.False(t, dest.RemoveForwardedParticipant("bc"))
	})

	t.Run("identity conflicts are rejected", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)

		// identity already joined in the destination
		dup, _ := newForwardSource(t, src, "p0")
		require.ErrorIs(t, dest.AddForwardedParticipant(dup, src, ForwardHooks{}), ErrAlreadyJoined)
		require.Nil(t, dest.GetForwardedParticipant("p0"))

		bc, _ := newForwardSource(t, src, "bc")
		require.ErrorIs(t, src.AddForwardedParticipant(bc, src, ForwardHooks{}), ErrForwardSameRoom)
		require.NoError(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}))
		require.ErrorIs(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}), ErrAlreadyForwarded)

		// a real participant cannot join under a forwarded identity
		joiner := NewMockParticipant("bc", types.CurrentProtocol, false, false, dest.LocalParticipantListener())
		require.ErrorIs(t, dest.Join(joiner, nil, nil, iceServersForRoom), ErrAlreadyJoined)

		// source that already left
		gone, _ := newForwardSource(t, src, "gone")
		src.RemoveParticipant(gone.Identity(), gone.ID(), types.ParticipantCloseReasonClientRequestLeave)
		require.ErrorIs(t, dest.AddForwardedParticipant(gone, src, ForwardHooks{}), ErrForwardSourceGone)
		require.Nil(t, dest.GetForwardedParticipant("gone"))

		// hidden participants are never exposed in another room
		hidden := NewMockParticipant("hidden", types.CurrentProtocol, true, false, src.LocalParticipantListener())
		require.NoError(t, src.Join(hidden, nil, &ParticipantOptions{}, iceServersForRoom))
		require.ErrorIs(t, dest.AddForwardedParticipant(hidden, src, ForwardHooks{}), ErrForwardHiddenParticipant)
		require.Nil(t, dest.GetForwardedParticipant("hidden"))
	})

	t.Run("a newer session of the same identity replaces the forward", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		src2 := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src2.Close(types.RoomCloseReasonUnknown)

		old, oldTrack := newForwardSource(t, src, "bc")
		probeOld := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(old, src, probeOld.hooks()))
		require.ErrorIs(t, dest.AddForwardedParticipant(old, src, ForwardHooks{}), ErrAlreadyForwarded)

		newer, newTrack := newForwardSource(t, src2, "bc")
		probeNew := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(newer, src2, probeNew.hooks()))

		require.True(t, probeOld.ended.Load())
		require.False(t, probeNew.ended.Load())
		require.Equal(t, newer, dest.GetForwardedParticipant("bc"))
		require.Empty(t, src.forwardTargetsOf(old.ID()))
		require.Len(t, src2.forwardTargetsOf(newer.ID()), 1)
		require.Nil(t, dest.trackManager.GetTrackInfo(oldTrack.ID()))
		require.NotNil(t, dest.trackManager.GetTrackInfo(newTrack.ID()))

		p0 := dest.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
		last := lastUpdateFor(p0, "bc")
		requireForwardedInfo(t, last, newer)

		// the old session leaving later does not disturb the new forward
		src.RemoveParticipant(old.Identity(), old.ID(), types.ParticipantCloseReasonClientRequestLeave)
		require.Equal(t, newer, dest.GetForwardedParticipant("bc"))
		require.False(t, probeNew.ended.Load())
	})

	t.Run("destination closing while the forward registers is rejected", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}

		// AddForwardedParticipant reads the identity once, then addForwardTarget reads it again: at that point
		// the entry is in the destination registry but not yet attached to the source room.
		calls := 0
		bc.IdentityCalls(func() livekit.ParticipantIdentity {
			calls++
			if calls == 2 {
				dest.Close(types.RoomCloseReasonUnknown)
			}
			return "bc"
		})
		err := dest.AddForwardedParticipant(bc, src, probe.hooks())
		bc.IdentityCalls(nil)
		require.ErrorIs(t, err, ErrRoomClosed)

		require.True(t, probe.ended.Load())
		require.Zero(t, probe.updates.Load())
		require.Empty(t, src.forwardTargetsOf(bc.ID()))
		require.Nil(t, dest.GetForwardedParticipant("bc"))
	})

	t.Run("updates racing the end of the forward are dropped", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))
		updatesBefore := probe.updates.Load()

		// the destination has looked the entry up; the forward ends while the update is being built
		calls := 0
		bc.ToProtoCalls(func() *livekit.ParticipantInfo {
			calls++
			if calls == 1 {
				require.True(t, dest.RemoveForwardedParticipant("bc"))
			}
			return &livekit.ParticipantInfo{Sid: string(bc.ID()), Identity: "bc", State: livekit.ParticipantInfo_ACTIVE, IsPublisher: true}
		})
		dest.onForwardedParticipantChanged(bc)
		bc.ToProtoCalls(nil)

		require.True(t, probe.ended.Load())
		require.Equal(t, updatesBefore, probe.updates.Load(), "OnUpdate must not run after OnEnd")
		p0 := dest.GetParticipants()[0].(*typesfakes.FakeLocalParticipant)
		last := lastUpdateFor(p0, "bc")
		require.NotNil(t, last)
		require.Equal(t, livekit.ParticipantInfo_DISCONNECTED, last.State, "no ACTIVE update after DISCONNECTED")
	})

	t.Run("destination stays open while a forward exists", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		dest.lock.Lock()
		dest.protoRoom.EmptyTimeout = 0
		dest.lock.Unlock()

		bc, _ := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))

		dest.CloseIfEmpty()
		require.False(t, dest.IsClosed())

		require.True(t, dest.RemoveForwardedParticipant("bc"))
		require.True(t, probe.ended.Load())
		require.Empty(t, src.forwardTargetsOf(bc.ID()))

		dest.CloseIfEmpty()
		require.True(t, dest.IsClosed())
	})

	t.Run("closing either room ends the forward", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		probe := &forwardProbe{}
		require.NoError(t, dest.AddForwardedParticipant(bc, src, probe.hooks()))

		dest.Close(types.RoomCloseReasonUnknown)
		require.True(t, probe.ended.Load())
		require.Empty(t, src.forwardTargetsOf(bc.ID()))
		require.ErrorIs(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}), ErrRoomClosed)

		dest2 := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest2.Close(types.RoomCloseReasonUnknown)
		probe2 := &forwardProbe{}
		require.NoError(t, dest2.AddForwardedParticipant(bc, src, probe2.hooks()))
		src.Close(types.RoomCloseReasonUnknown)
		require.True(t, probe2.ended.Load())
		require.Nil(t, dest2.GetForwardedParticipant("bc"))
	})

	t.Run("forwarded publisher is an active speaker in the destination", func(t *testing.T) {
		dest := newRoomWithParticipants(t, testRoomOpts{num: 1})
		defer dest.Close(types.RoomCloseReasonUnknown)
		src := newRoomWithParticipants(t, testRoomOpts{num: 0})
		defer src.Close(types.RoomCloseReasonUnknown)
		bc, _ := newForwardSource(t, src, "bc")
		require.NoError(t, dest.AddForwardedParticipant(bc, src, ForwardHooks{}))

		bc.GetAudioLevelReturns(0.5, true)
		speakers := dest.GetActiveSpeakers()
		require.Len(t, speakers, 1)
		require.Equal(t, string(bc.ID()), speakers[0].Sid)
		require.True(t, speakers[0].Active)
	})
}
