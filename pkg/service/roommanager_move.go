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
	"time"

	"github.com/pkg/errors"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/protocol/utils/must"

	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// Argon: same-node participant move, service side. The SFU side lives in pkg/rtc/room_move.go.
//
// The participant keeps its session (transport, signal connection, published tracks) and its grants; only
// Video.Room changes. The client gets a RoomMovedResponse whose token, signed with the API key that admitted
// the participant, is used for reconnects to the destination. Permissions are not touched by the move:
// Argon calls UpdateParticipant on the destination right after it.

func (r *RoomManager) moveParticipant(ctx context.Context, req *livekit.MoveParticipantRequest) (*livekit.MoveParticipantResponse, error) {
	srcRoom, p, err := r.roomAndParticipantForReq(ctx, req)
	if err != nil {
		return nil, r.forwardAwareErr(ctx, req, err)
	}

	destName := livekit.RoomName(req.DestinationRoom)
	if destName == "" {
		return nil, ErrNoRoomName
	}
	if destName == srcRoom.Name() {
		return nil, ErrDestinationSameAsSourceRoom
	}
	if !r.config.Limit.CheckRoomNameLength(string(destName)) {
		return nil, ErrRoomNameExceedsLimits
	}
	if err := p.SupportsMoving(); err != nil {
		return nil, err
	}

	apiKey, secret, err := r.moveSigningKey(srcRoom, p)
	if err != nil {
		return nil, err
	}

	dest, err := r.getOrCreateLocalDestination(ctx, srcRoom, destName, ErrMoveCrossNode)
	if err != nil {
		return nil, err
	}
	defer dest.Release()

	identity := p.Identity()
	if dest.GetParticipant(identity) != nil || dest.GetForwardedParticipant(identity) != nil {
		return nil, ErrMoveIdentityInUse
	}

	token, err := moveToken(p, destName, apiKey, secret)
	if err != nil {
		return nil, err
	}

	// the same helper type as StartSession: MoveToRoom stores it in an atomic.Value
	if err := srcRoom.MoveParticipant(dest, p, rtc.MoveParticipantParams{
		ParticipantID: livekit.ParticipantID(guid.New(utils.ParticipantPrefix)),
		Token:         token,
		Helper: &roomManagerParticipantHelper{
			room:                     dest,
			codecRegressionThreshold: r.config.Video.CodecRegressionThreshold,
		},
	}); err != nil {
		switch {
		case errors.Is(err, rtc.ErrAlreadyJoined):
			return nil, ErrMoveIdentityInUse
		case errors.Is(err, rtc.ErrMoveSourceGone):
			return nil, ErrParticipantNotFound
		case errors.Is(err, rtc.ErrRoomClosed):
			return nil, ErrRoomNotFound
		}
		return nil, err
	}

	// MoveToRoom ran the session's close callbacks: source topic, store record and participant_left are
	// done. The bookkeeping below outlives this RPC, like those callbacks do.
	ctx = context.WithoutCancel(ctx)
	if err := r.roomStore.DeleteParticipant(ctx, srcRoom.Name(), identity); err != nil {
		p.GetLogger().Errorw("could not delete moved participant from source room", err)
	}
	if err := r.bindMovedParticipant(ctx, dest, p); err != nil {
		return nil, err
	}
	p.GetLogger().Infow("participant moved", "sourceRoom", srcRoom.Name(), "destinationRoom", destName)
	return &livekit.MoveParticipantResponse{}, nil
}

// bindMovedParticipant does what StartSession does after Join: route participant RPCs for
// (destination, identity) to this node, keep the store current and clean up when the participant leaves.
func (r *RoomManager) bindMovedParticipant(ctx context.Context, room *rtc.Room, p types.LocalParticipant) error {
	roomName, identity := room.Name(), p.Identity()
	pLogger := p.GetLogger()

	participantTopic := rpc.FormatParticipantTopic(roomName, identity)
	participantServer := must.Get(rpc.NewTypedParticipantServer(r, r.bus))
	kill := r.participantServers.Replace(participantTopic, participantServer)
	if err := participantServer.RegisterAllParticipantTopics(participantTopic); err != nil {
		kill()
		pLogger.Errorw("could not register moved participant topic", err)
		_ = p.Close(true, types.ParticipantCloseReasonMessageBusFailed, false)
		return err
	}

	if err := r.roomStore.StoreParticipant(ctx, roomName, p.ToProto()); err != nil {
		pLogger.Errorw("could not store moved participant", err)
	}
	persistRoomForParticipantCount := func() {
		if !p.Hidden() && !room.IsClosed() {
			if err := r.roomStore.StoreRoom(ctx, room.ToProto(), room.Internal()); err != nil {
				pLogger.Errorw("could not store room", err)
			}
		}
	}
	persistRoomForParticipantCount()

	p.AddOnClose(types.ParticipantCloseKeyNormal, func(p types.LocalParticipant) {
		kill()
		if err := r.roomStore.DeleteParticipant(ctx, roomName, identity); err != nil {
			pLogger.Errorw("could not delete participant", err)
		}
		persistRoomForParticipantCount()
		r.telemetry.ParticipantLeft(ctx, room.ToProto(), leftParticipantInfo(p), true, p.TelemetryGuard())
	})
	p.OnICEConfigChanged(func(p types.LocalParticipant, iceConfig *livekit.ICEConfig) {
		r.iceConfigCache.Put(iceConfigCacheKey{roomName, identity}, iceConfig)
	})
	return nil
}

// moveSigningKey is the key the participant was admitted with, else the key its room was created with
// (webhooks route by it too). Without a configured key the move is refused rather than signed arbitrarily.
func (r *RoomManager) moveSigningKey(room *rtc.Room, p types.LocalParticipant) (string, string, error) {
	apiKey := p.APIKey()
	if apiKey == "" {
		apiKey = room.Internal().GetTags()[RoomAPIKeyTag]
	}
	if apiKey == "" {
		return "", "", ErrMoveNoSigningKey
	}
	// config.Keys is what the auth.KeyProvider is built from, see createKeyProvider
	secret := r.config.Keys[apiKey]
	if secret == "" {
		return "", "", ErrMoveNoSigningKey
	}
	return apiKey, secret, nil
}

// moveToken re-issues the participant's current claims for the destination. Like refreshed tokens it is
// valid for what is left of the original token, but at least tokenDefaultTTL.
func moveToken(p types.LocalParticipant, dest livekit.RoomName, apiKey, secret string) (string, error) {
	grants := p.ClaimGrants().Clone()
	if grants.Video == nil {
		grants.Video = &auth.VideoGrant{}
	}
	grants.Video.Room = string(dest)
	grants.Video.DestinationRoom = ""

	validFor := tokenDefaultTTL
	if expiresAt := p.TokenExpiresAt(); !expiresAt.IsZero() {
		if remaining := time.Until(expiresAt); remaining > validFor {
			validFor = remaining
		}
	}

	return auth.NewAccessToken(apiKey, secret).
		SetName(grants.Name).
		SetIdentity(string(p.Identity())).
		SetKind(grants.GetParticipantKind()).
		SetValidFor(validFor).
		SetMetadata(grants.Metadata).
		SetAttributes(grants.Attributes).
		SetVideoGrant(grants.Video).
		SetRoomConfig(grants.GetRoomConfiguration()).
		SetRoomPreset(grants.RoomPreset).
		ToJWT()
}

// leftParticipantInfo is what a session's close callback reports as left. A participant whose close
// callbacks run while it is still connected is being moved: report it as disconnected, moved.
func leftParticipantInfo(p types.LocalParticipant) *livekit.ParticipantInfo {
	pi := utils.CloneProto(p.ToProto())
	if pi.State != livekit.ParticipantInfo_DISCONNECTED {
		pi.State = livekit.ParticipantInfo_DISCONNECTED
		pi.DisconnectReason = rtc.MovedDisconnectReason
	}
	return pi
}

// participantRequestSource looks a session up in its current room: a moved participant is no longer in
// the room its session started in.
func (r *RoomManager) participantRequestSource(room *rtc.Room, p types.LocalParticipant) routing.MessageSource {
	identity := p.Identity()
	if cur := room.GetParticipant(identity); cur == nil || cur.ID() != p.ID() {
		if grants := p.ClaimGrants(); grants != nil && grants.Video != nil {
			if moved := r.GetRoom(context.Background(), livekit.RoomName(grants.Video.Room)); moved != nil {
				room = moved
			}
		}
	}
	return room.GetParticipantRequestSource(identity)
}
