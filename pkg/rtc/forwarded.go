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
	"maps"
	"slices"
	"sync"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// Argon: same-node participant forwarding.
//
// A forwarded participant is a lightweight proxy in a destination room for a participant that lives in a
// source room on this node. The proxy shares the source's MediaTrack objects, so destination subscribers get
// DownTracks exactly as in-room subscribers do. Only media tracks are forwarded: no data packets, no RPC and
// no subscriptions on behalf of the forwarded participant. Hidden participants cannot be forwarded.
//
// Behaviour that differs from LiveKit Cloud (or is unspecified there):
//   - forwarded publishers count as occupancy for the destination's empty timeout (CloseIfEmpty),
//   - they are not counted in the destination Room proto's NumParticipants/NumPublishers,
//   - no participant_joined/participant_left/track_published telemetry is emitted for them in the destination.
//
// Locking: Room.lock guards the two registries (forwarded, forwardTargets). A room never holds its lock while
// calling into another room: every cross-room step snapshots under the local lock, releases it, then acts.
// forwardedParticipant.lock and batchedUpdatesMu are leaf locks; Room.lock is never taken while holding them.

// ForwardHooks lets the service layer follow a forward's lifecycle in the destination room.
type ForwardHooks struct {
	// OnUpdate receives a fresh copy of the forwarded ParticipantInfo whenever it changes, including on add.
	// It is never called after OnEnd.
	OnUpdate func(pi *livekit.ParticipantInfo)
	// OnEnd is called exactly once when the forward ends for any reason.
	OnEnd func()
}

type forwardedParticipant struct {
	src     types.LocalParticipant
	srcRoom *Room
	hooks   ForwardHooks

	// lock serializes track bookkeeping, updates and the end of the forward
	lock   sync.Mutex
	tracks map[livekit.TrackID]types.MediaTrack // tracks registered in the destination RoomTrackManager
	ended  bool
}

// addTrack registers a track; returns false once the forward has ended.
func (fp *forwardedParticipant) addTrack(track types.MediaTrack) bool {
	fp.lock.Lock()
	defer fp.lock.Unlock()
	if fp.ended {
		return false
	}
	fp.tracks[track.ID()] = track
	return true
}

func (fp *forwardedParticipant) removeTrack(track types.MediaTrack) {
	fp.lock.Lock()
	if fp.tracks[track.ID()] == track {
		delete(fp.tracks, track.ID())
	}
	fp.lock.Unlock()
}

func (fp *forwardedParticipant) getTracks() []types.MediaTrack {
	fp.lock.Lock()
	defer fp.lock.Unlock()
	return slices.Collect(maps.Values(fp.tracks))
}

func (fp *forwardedParticipant) isEnded() bool {
	fp.lock.Lock()
	defer fp.lock.Unlock()
	return fp.ended
}

// forwardedProto returns a copy of the source's ParticipantInfo marked FORWARDED. The source's proto is never mutated.
func forwardedProto(src types.Participant) *livekit.ParticipantInfo {
	pi := utils.CloneProto(src.ToProto())
	if !slices.Contains(pi.KindDetails, livekit.ParticipantInfo_FORWARDED) {
		pi.KindDetails = append(pi.KindDetails, livekit.ParticipantInfo_FORWARDED)
	}
	if pi.State != livekit.ParticipantInfo_DISCONNECTED {
		pi.State = livekit.ParticipantInfo_ACTIVE
	}
	return pi
}

// ------------------------------------------------------------
// destination side

// AddForwardedParticipant makes src, a participant of srcRoom on this node, visible in this room as a
// forwarded publisher. Its published tracks become subscribable here and follow the source afterwards.
// A forward of a newer session of the same identity replaces the older one.
func (r *Room) AddForwardedParticipant(src types.LocalParticipant, srcRoom *Room, hooks ForwardHooks) error {
	if srcRoom == r {
		return ErrForwardSameRoom
	}
	if src.Hidden() {
		// Argon: hidden participants are never exposed in another room
		return ErrForwardHiddenParticipant
	}
	identity := src.Identity()

	r.lock.Lock()
	if r.IsClosed() {
		r.lock.Unlock()
		return ErrRoomClosed
	}
	if r.participants[identity] != nil {
		r.lock.Unlock()
		return ErrAlreadyJoined
	}
	stale := r.forwarded[identity]
	if stale != nil && stale.src.ID() == src.ID() {
		r.lock.Unlock()
		return ErrAlreadyForwarded
	}
	fp := &forwardedParticipant{
		src:     src,
		srcRoom: srcRoom,
		hooks:   hooks,
		tracks:  make(map[livekit.TrackID]types.MediaTrack),
	}
	r.forwarded[identity] = fp
	r.lock.Unlock()

	if stale != nil {
		stale.srcRoom.removeForwardTarget(stale.src.ID(), r)
		r.finishForward(stale, true)
	}

	if !srcRoom.addForwardTarget(src, r) {
		r.lock.Lock()
		if r.forwarded[identity] == fp {
			delete(r.forwarded, identity)
		}
		r.lock.Unlock()
		return ErrForwardSourceGone
	}
	if fp.isEnded() {
		// the room closed while the forward was being registered
		srcRoom.removeForwardTarget(src.ID(), r)
		return ErrRoomClosed
	}

	r.logger.Infow("forwarded participant added", "participant", identity, "pID", src.ID(), "sourceRoom", srcRoom.Name())

	tracks := src.GetPublishedTracks()
	for _, track := range tracks {
		r.addForwardedTrack(fp, track)
	}
	r.forwardedStateChanged(fp)
	for _, track := range tracks {
		r.subscribeToForwardedTrack(track)
	}
	return nil
}

// RemoveForwardedParticipant ends a forward into this room. Returns false if identity is not forwarded here.
func (r *Room) RemoveForwardedParticipant(identity livekit.ParticipantIdentity) bool {
	r.lock.Lock()
	fp := r.forwarded[identity]
	if fp == nil {
		r.lock.Unlock()
		return false
	}
	delete(r.forwarded, identity)
	r.lock.Unlock()

	fp.srcRoom.removeForwardTarget(fp.src.ID(), r)
	r.finishForward(fp, true)
	return true
}

// GetForwardedParticipant returns the source participant forwarded into this room under identity, if any.
func (r *Room) GetForwardedParticipant(identity livekit.ParticipantIdentity) types.LocalParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if fp := r.forwarded[identity]; fp != nil {
		return fp.src
	}
	return nil
}

// GetForwardedParticipantInfo returns the FORWARDED ParticipantInfo for identity, or nil.
func (r *Room) GetForwardedParticipantInfo(identity livekit.ParticipantIdentity) *livekit.ParticipantInfo {
	if src := r.GetForwardedParticipant(identity); src != nil {
		return forwardedProto(src)
	}
	return nil
}

// GetForwardedParticipantInfoByID returns the FORWARDED ParticipantInfo for a participant SID, or nil.
func (r *Room) GetForwardedParticipantInfoByID(pID livekit.ParticipantID) *livekit.ParticipantInfo {
	if src := r.getForwardedSourceByID(pID); src != nil {
		return forwardedProto(src)
	}
	return nil
}

// GetForwardedParticipantInfos returns FORWARDED ParticipantInfos for every participant forwarded into this room.
func (r *Room) GetForwardedParticipantInfos() []*livekit.ParticipantInfo {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.forwardedInfosLocked()
}

func (r *Room) forwardedInfosLocked() []*livekit.ParticipantInfo {
	if len(r.forwarded) == 0 {
		return nil
	}
	infos := make([]*livekit.ParticipantInfo, 0, len(r.forwarded))
	for _, fp := range r.forwarded {
		if fp.src.Hidden() {
			continue
		}
		infos = append(infos, forwardedProto(fp.src))
	}
	return infos
}

func (r *Room) forwardedSources() []types.LocalParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if len(r.forwarded) == 0 {
		return nil
	}
	sources := make([]types.LocalParticipant, 0, len(r.forwarded))
	for _, fp := range r.forwarded {
		sources = append(sources, fp.src)
	}
	return sources
}

func (r *Room) forwardedTrackIDs() []livekit.TrackID {
	r.lock.RLock()
	fps := slices.Collect(maps.Values(r.forwarded))
	r.lock.RUnlock()

	var trackIDs []livekit.TrackID
	for _, fp := range fps {
		for _, track := range fp.getTracks() {
			trackIDs = append(trackIDs, track.ID())
		}
	}
	return trackIDs
}

func (r *Room) getForwardedSourceByID(pID livekit.ParticipantID) types.LocalParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, fp := range r.forwarded {
		if fp.src.ID() == pID {
			return fp.src
		}
	}
	return nil
}

// getPublisherByID resolves a publisher by SID, including participants forwarded into this room.
func (r *Room) getPublisherByID(pID livekit.ParticipantID) types.LocalParticipant {
	if p := r.GetParticipantByID(pID); p != nil {
		return p
	}
	return r.getForwardedSourceByID(pID)
}

// getForwardedFor returns the registry entry for p if this exact participant session is forwarded here.
func (r *Room) getForwardedFor(p types.Participant) *forwardedParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if fp := r.forwarded[p.Identity()]; fp != nil && fp.src.ID() == p.ID() {
		return fp
	}
	return nil
}

func (r *Room) addForwardedTrack(fp *forwardedParticipant, track types.MediaTrack) {
	if !fp.addTrack(track) {
		return
	}
	r.trackManager.AddTrack(track, fp.src.Identity(), fp.src.ID())
	if fp.isEnded() {
		// lost the race with the forward ending
		r.trackManager.RemoveTrack(track)
	}
}

// subscribeToForwardedTrack mirrors onTrackPublished for destination participants.
func (r *Room) subscribeToForwardedTrack(track types.MediaTrack) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, p := range r.participants {
		if p.State() != livekit.ParticipantInfo_ACTIVE || !r.autoSubscribe(p) {
			continue
		}
		p.SubscribeToTrack(track.ID(), false)
	}
}

// forwardedStateChanged publishes the current forwarded proto, unless the forward has ended meanwhile.
func (r *Room) forwardedStateChanged(fp *forwardedParticipant) {
	pi := forwardedProto(fp.src)
	participants := r.GetParticipants()

	fp.lock.Lock()
	defer fp.lock.Unlock()
	if fp.ended {
		return
	}
	if fp.hooks.OnUpdate != nil {
		fp.hooks.OnUpdate(utils.CloneProto(pi))
	}
	r.sendForwardedState(pi, participants)
}

func (r *Room) sendForwardedState(pi *livekit.ParticipantInfo, participants []types.LocalParticipant) {
	r.batchedUpdatesMu.Lock()
	updates := PushAndDequeueUpdates(pi, types.ParticipantCloseReasonNone, true, nil, r.batchedUpdates)
	r.batchedUpdatesMu.Unlock()
	SendParticipantUpdates(updates, participants, r.roomConfig.UpdateBatchTargetSize)
}

// finishForward tears down a forward already removed from the registry. Runs at most once per entry.
func (r *Room) finishForward(fp *forwardedParticipant, notify bool) {
	var participants []types.LocalParticipant
	if notify {
		participants = r.GetParticipants()
	}

	fp.lock.Lock()
	if fp.ended {
		fp.lock.Unlock()
		return
	}
	fp.ended = true
	tracks := slices.Collect(maps.Values(fp.tracks))
	if notify {
		pi := forwardedProto(fp.src)
		pi.State = livekit.ParticipantInfo_DISCONNECTED
		if pi.DisconnectReason == livekit.DisconnectReason_UNKNOWN_REASON {
			pi.DisconnectReason = livekit.DisconnectReason_PARTICIPANT_REMOVED
		}
		r.sendForwardedState(pi, participants)
	}
	fp.lock.Unlock()

	for _, track := range tracks {
		r.trackManager.RemoveTrack(track)
	}
	r.logger.Infow("forwarded participant removed", "participant", fp.src.Identity(), "pID", fp.src.ID(), "sourceRoom", fp.srcRoom.Name())
	if fp.hooks.OnEnd != nil {
		fp.hooks.OnEnd()
	}
}

// forwardEndedBySource is called by the source room when that participant session leaves or its room closes.
func (r *Room) forwardEndedBySource(pID livekit.ParticipantID) {
	r.lock.Lock()
	var fp *forwardedParticipant
	for identity, candidate := range r.forwarded {
		if candidate.src.ID() == pID {
			fp = candidate
			delete(r.forwarded, identity)
			break
		}
	}
	r.lock.Unlock()

	if fp != nil {
		r.finishForward(fp, true)
	}
}

func (r *Room) onForwardedTrackPublished(p types.Participant, track types.MediaTrack) {
	fp := r.getForwardedFor(p)
	if fp == nil {
		return
	}
	r.addForwardedTrack(fp, track)
	r.forwardedStateChanged(fp)
	r.subscribeToForwardedTrack(track)
}

func (r *Room) onForwardedTrackUnpublished(p types.Participant, track types.MediaTrack) {
	fp := r.getForwardedFor(p)
	if fp == nil {
		return
	}
	fp.removeTrack(track)
	r.trackManager.RemoveTrack(track)
	if !p.IsClosed() {
		r.forwardedStateChanged(fp)
	}
}

func (r *Room) onForwardedParticipantChanged(p types.Participant) {
	if fp := r.getForwardedFor(p); fp != nil {
		r.forwardedStateChanged(fp)
	}
}

func (r *Room) onForwardedTracksChanged(p types.Participant) {
	fp := r.getForwardedFor(p)
	if fp == nil {
		return
	}
	for _, track := range fp.getTracks() {
		r.trackManager.NotifyTrackChanged(track.ID())
	}
}

// closeForwards drops every forward this room takes part in, as destination and as source. Called from Close.
func (r *Room) closeForwards() {
	r.lock.Lock()
	forwarded := r.forwarded
	targets := r.forwardTargets
	r.forwarded = make(map[livekit.ParticipantIdentity]*forwardedParticipant)
	r.forwardTargets = make(map[livekit.ParticipantID]map[*Room]struct{})
	r.lock.Unlock()

	for _, fp := range forwarded {
		fp.srcRoom.removeForwardTarget(fp.src.ID(), r)
		r.finishForward(fp, false)
	}
	for pID, dests := range targets {
		for dest := range dests {
			dest.forwardEndedBySource(pID)
		}
	}
}

// ------------------------------------------------------------
// source side, keyed by participant SID so that a newer session of the same identity is never confused with an older one

// addForwardTarget registers dest as a forward target of src. Returns false if src is no longer in this room.
func (r *Room) addForwardTarget(src types.LocalParticipant, dest *Room) bool {
	identity := src.Identity()
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.IsClosed() {
		return false
	}
	if p := r.participants[identity]; p == nil || p.ID() != src.ID() || p.IsDisconnected() {
		return false
	}
	targets := r.forwardTargets[src.ID()]
	if targets == nil {
		targets = make(map[*Room]struct{})
		r.forwardTargets[src.ID()] = targets
	}
	targets[dest] = struct{}{}
	return true
}

func (r *Room) removeForwardTarget(pID livekit.ParticipantID, dest *Room) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if targets := r.forwardTargets[pID]; targets != nil {
		delete(targets, dest)
		if len(targets) == 0 {
			delete(r.forwardTargets, pID)
		}
	}
}

func (r *Room) forwardTargetsOf(pID livekit.ParticipantID) []*Room {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if len(r.forwardTargets[pID]) == 0 {
		return nil
	}
	return slices.Collect(maps.Keys(r.forwardTargets[pID]))
}

// unforwardAll ends every forward of a participant session out of this room. Called from RemoveParticipant.
func (r *Room) unforwardAll(pID livekit.ParticipantID) {
	r.lock.Lock()
	targets := r.forwardTargets[pID]
	delete(r.forwardTargets, pID)
	r.lock.Unlock()

	for dest := range targets {
		dest.forwardEndedBySource(pID)
	}
}

func (r *Room) forwardTrackPublished(p types.Participant, track types.MediaTrack) {
	for _, dest := range r.forwardTargetsOf(p.ID()) {
		dest.onForwardedTrackPublished(p, track)
	}
}

func (r *Room) forwardTrackUnpublished(p types.Participant, track types.MediaTrack) {
	for _, dest := range r.forwardTargetsOf(p.ID()) {
		dest.onForwardedTrackUnpublished(p, track)
	}
}

func (r *Room) forwardParticipantChanged(p types.Participant) {
	for _, dest := range r.forwardTargetsOf(p.ID()) {
		dest.onForwardedParticipantChanged(p)
	}
}

func (r *Room) forwardTracksChanged(p types.Participant) {
	for _, dest := range r.forwardTargetsOf(p.ID()) {
		dest.onForwardedTracksChanged(p)
	}
}
