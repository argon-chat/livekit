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
	"context"
	"maps"
	"slices"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// Argon: same-node participant move, the room side of ParticipantImpl.MoveToRoom.
//
// The participant is detached from the source (registries, tracks, forwards; source participants see it
// DISCONNECTED), MoveToRoom rebinds it (the source's close callbacks and telemetry fire, new SID, listeners
// and helper of the destination), then it is attached to the destination like a join without a JoinResponse:
// RoomMovedResponse carries room, token, participant and the other participants. Published tracks, the
// transport and the signal connection survive; subscriptions are re-established in the destination.

// MovedDisconnectReason is what the source room reports for a participant that moved out.
const MovedDisconnectReason = livekit.DisconnectReason_MIGRATION

type MoveParticipantParams struct {
	// ParticipantID is the participant's SID in the destination
	ParticipantID livekit.ParticipantID
	// Token lets the client reconnect to the destination
	Token  string
	Helper types.LocalParticipantHelper
}

// MoveParticipant moves p, a participant of this room, into dest.
func (r *Room) MoveParticipant(dest *Room, p types.LocalParticipant, params MoveParticipantParams) error {
	if dest == r {
		return ErrMoveSameRoom
	}
	if err := dest.checkMovedIn(p); err != nil {
		return err
	}

	opts, requestSource, err := r.detachMovedOut(p)
	if err != nil {
		return err
	}
	previousID := p.ID()

	p.MoveToRoom(types.MoveToRoomParams{
		RoomName:          dest.Name(),
		ParticipantID:     params.ParticipantID,
		Listener:          dest.LocalParticipantListener(),
		TelemetryListener: dest.ParticipantTelemetryListener(),
		Helper:            params.Helper,
	})

	if err := dest.attachMovedIn(p, opts, requestSource, params.Token); err != nil {
		// already out of the source, do not leave the participant in limbo
		p.GetLogger().Warnw("could not attach moved participant", err, "destinationRoom", dest.Name())
		_ = p.Close(true, types.ParticipantCloseReasonMoveFailed, false)
		return err
	}

	dest.logger.Infow(
		"participant moved in",
		"participant", p.Identity(),
		"pID", p.ID(),
		"previousPID", previousID,
		"sourceRoom", r.Name(),
	)
	return nil
}

// detachMovedOut takes p out of this room without closing it; the others see a normal leave.
func (r *Room) detachMovedOut(p types.LocalParticipant) (*ParticipantOptions, routing.MessageSource, error) {
	identity := p.Identity()

	r.lock.Lock()
	if cur := r.participants[identity]; cur == nil || cur.ID() != p.ID() || p.IsDisconnected() {
		r.lock.Unlock()
		return nil, nil, ErrMoveSourceGone
	}
	opts := r.participantOpts[identity]
	requestSource := r.participantRequestSources[identity]
	delete(r.participants, identity)
	delete(r.participantOpts, identity)
	delete(r.participantRequestSources, identity)
	delete(r.hasPublished, identity)
	delete(r.launchedTrackEgresses, identity)
	if !p.Hidden() {
		r.protoRoom.NumParticipants--
	}
	r.lock.Unlock()
	r.protoProxy.MarkDirty(false)

	for _, t := range p.GetPublishedTracks() {
		r.trackManager.RemoveTrack(t)
	}
	for _, t := range p.GetPublishedDataTracks() {
		r.trackManager.RemoveDataTrack(t)
	}
	r.unforwardAll(p.ID())
	p.ClearParticipantListener()
	r.leftAt.Store(time.Now().Unix())

	if !p.Hidden() {
		pi := utils.CloneProto(p.ToProto())
		pi.State = livekit.ParticipantInfo_DISCONNECTED
		pi.DisconnectReason = MovedDisconnectReason
		r.batchedUpdatesMu.Lock()
		updates := PushAndDequeueUpdates(pi, types.ParticipantCloseReasonNone, true, nil, r.batchedUpdates)
		r.batchedUpdatesMu.Unlock()
		SendParticipantUpdates(updates, r.GetParticipants(), r.roomConfig.UpdateBatchTargetSize)
	}
	return opts, requestSource, nil
}

func (r *Room) checkMovedIn(p types.LocalParticipant) error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.checkMovedInLocked(p)
}

func (r *Room) checkMovedInLocked(p types.LocalParticipant) error {
	if r.IsClosed() {
		return ErrRoomClosed
	}
	if r.participants[p.Identity()] != nil || r.forwarded[p.Identity()] != nil {
		return ErrAlreadyJoined
	}
	if r.protoRoom.MaxParticipants > 0 && !p.IsDependent() {
		numParticipants := uint32(0)
		for _, op := range r.participants {
			if !op.IsDependent() {
				numParticipants++
			}
		}
		if numParticipants >= r.protoRoom.MaxParticipants {
			return ErrMaxParticipantsExceeded
		}
	}
	return nil
}

// attachMovedIn adds a participant that MoveToRoom rebound to this room, mirrors Join.
func (r *Room) attachMovedIn(
	p types.LocalParticipant,
	opts *ParticipantOptions,
	requestSource routing.MessageSource,
	token string,
) error {
	identity := p.Identity()
	tracks := p.GetPublishedTracks()
	dataTracks := p.GetPublishedDataTracks()

	r.lock.Lock()
	if err := r.checkMovedInLocked(p); err != nil {
		r.lock.Unlock()
		return err
	}
	if r.FirstJoinedAt() == 0 && !p.IsDependent() {
		r.joinedAt.Store(time.Now().Unix())
	}
	dispatches := slices.Collect(maps.Values(r.agentDispatches))
	r.launchTargetAgents(dispatches, p, livekit.JobType_JT_PARTICIPANT)
	if len(tracks) > 0 {
		r.hasPublished[identity] = true
		r.launchTargetAgents(dispatches, p, livekit.JobType_JT_PUBLISHER)
	}
	if p.IsRecorder() && !r.protoRoom.ActiveRecording {
		r.protoRoom.ActiveRecording = true
		r.protoProxy.MarkDirty(true)
	} else {
		r.protoProxy.MarkDirty(false)
	}
	r.participants[identity] = p
	r.participantOpts[identity] = opts
	r.participantRequestSources[identity] = requestSource
	if r.onParticipantChanged != nil {
		r.onParticipantChanged(p)
	}
	moved := &livekit.RoomMovedResponse{
		Room:        r.ToProto(),
		Token:       token,
		Participant: p.ToProto(),
		OtherParticipants: GetOtherParticipantInfo(
			p,
			false, // isMigratingIn
			toParticipants(slices.Collect(maps.Values(r.participants))),
			false, // skipSubscriberBroadcast
		),
	}
	moved.OtherParticipants = append(moved.OtherParticipants, r.forwardedInfosLocked()...)
	r.lock.Unlock()

	if err := p.SendRoomMovedResponse(moved); err != nil {
		return err
	}

	// the destination session starts here; the tracks are published again in it
	ctx := context.Background()
	clientMeta := &livekit.AnalyticsClientMeta{Region: r.serverInfo.GetRegion(), Node: r.serverInfo.GetNodeId()}
	r.telemetry.ParticipantJoined(ctx, r.ToProto(), p.ToProto(), p.GetClientInfo(), clientMeta, true, p.TelemetryGuard())
	r.telemetry.ParticipantActive(ctx, r.ToProto(), p.ToProto(), clientMeta, false, p.IsWarpEnabled(), p.TelemetryGuard())
	for _, track := range tracks {
		r.trackManager.AddTrack(track, identity, p.ID())
		r.participantTelemetryListener.OnTrackPublished(p.ID(), identity, track.ToProto(), true)
	}
	for _, dt := range dataTracks {
		r.trackManager.AddDataTrack(dt, identity, p.ID())
	}
	r.broadcastParticipantState(p, broadcastOptions{skipSource: true, immediate: true})

	r.lock.RLock()
	for _, op := range r.participants {
		if op.ID() == p.ID() || op.State() != livekit.ParticipantInfo_ACTIVE {
			continue
		}
		if r.autoSubscribe(op) {
			for _, track := range tracks {
				op.SubscribeToTrack(track.ID(), false)
			}
		}
		if r.autoSubscribeDataTrack(op) {
			for _, dt := range dataTracks {
				op.SubscribeToDataTrack(dt.ID())
			}
		}
	}
	r.lock.RUnlock()
	r.subscribeToExistingTracks(p, false)
	return nil
}
