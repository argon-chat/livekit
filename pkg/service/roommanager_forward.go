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
	"sync"

	"github.com/pkg/errors"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/must"

	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/selector"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// Argon: same-node participant forwarding, service side. The SFU side lives in pkg/rtc/forwarded.go.
//
// A forward registers the (destination room, identity) participant topic on this node so that
// RemoveParticipant/MutePublishedTrack/GetParticipant addressed to the destination reach us, and stores a
// FORWARDED ParticipantInfo for the destination room so ListParticipants/GetParticipant return it. Both are
// undone when the forward ends for any reason (source left, destination closed, explicit RemoveParticipant).

// forwardCloser runs a cleanup function once, whether it is set before or after close is requested.
type forwardCloser struct {
	mu     sync.Mutex
	closed bool
	fn     func()
}

func (c *forwardCloser) set(fn func()) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		fn()
		return
	}
	c.fn = fn
	c.mu.Unlock()
}

func (c *forwardCloser) close() {
	c.mu.Lock()
	fn := c.fn
	c.fn = nil
	c.closed = true
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (r *RoomManager) forwardParticipant(ctx context.Context, req *livekit.ForwardParticipantRequest) (*livekit.ForwardParticipantResponse, error) {
	srcRoom, src, err := r.roomAndParticipantForReq(ctx, req)
	if err != nil {
		return nil, err
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

	identity := src.Identity()
	if dest := r.GetRoom(ctx, destName); dest != nil {
		if existing := dest.GetForwardedParticipant(identity); existing != nil && existing.ID() == src.ID() {
			return &livekit.ForwardParticipantResponse{}, nil
		}
	}

	dest, err := r.getOrCreateForwardDestination(ctx, srcRoom, destName)
	if err != nil {
		return nil, err
	}
	defer dest.Release()

	// hooks outlive this RPC
	storeCtx := context.WithoutCancel(ctx)
	closer := &forwardCloser{}
	hooks := rtc.ForwardHooks{
		OnUpdate: func(pi *livekit.ParticipantInfo) {
			if err := r.roomStore.StoreParticipant(storeCtx, destName, pi); err != nil {
				dest.Logger().Errorw("could not store forwarded participant", err, "participant", identity)
			}
		},
		OnEnd: func() {
			closer.close()
			if err := r.roomStore.DeleteParticipant(storeCtx, destName, identity); err != nil {
				dest.Logger().Errorw("could not delete forwarded participant", err, "participant", identity)
			}
		},
	}
	if err := dest.AddForwardedParticipant(src, srcRoom, hooks); err != nil {
		switch {
		case errors.Is(err, rtc.ErrAlreadyForwarded):
			return &livekit.ForwardParticipantResponse{}, nil
		case errors.Is(err, rtc.ErrAlreadyJoined):
			return nil, ErrForwardIdentityInUse
		case errors.Is(err, rtc.ErrForwardSourceGone):
			return nil, ErrParticipantNotFound
		case errors.Is(err, rtc.ErrForwardHiddenParticipant):
			return nil, ErrForwardHiddenParticipant
		case errors.Is(err, rtc.ErrRoomClosed):
			return nil, ErrRoomNotFound
		}
		return nil, err
	}

	// route participant RPCs addressed to (destination, identity) to this node, mirrors StartSession
	participantTopic := rpc.FormatParticipantTopic(destName, identity)
	participantServer := must.Get(rpc.NewTypedParticipantServer(r, r.bus))
	kill := r.participantServers.Replace(participantTopic, participantServer)
	if err := participantServer.RegisterAllParticipantTopics(participantTopic); err != nil {
		kill()
		dest.RemoveForwardedParticipant(identity)
		src.GetLogger().Errorw("could not register forwarded participant topic", err, "destinationRoom", destName)
		return nil, err
	}
	closer.set(kill)

	src.GetLogger().Infow("participant forwarded", "destinationRoom", destName)
	return &livekit.ForwardParticipantResponse{}, nil
}

// getOrCreateForwardDestination returns a held destination room hosted on this node, creating it when allowed.
func (r *RoomManager) getOrCreateForwardDestination(ctx context.Context, srcRoom *rtc.Room, destName livekit.RoomName) (*rtc.Room, error) {
	if room := r.GetRoom(ctx, destName); room != nil && room.Hold() {
		return room, nil
	}

	// Argon: forwarding is same-node only, refuse when another live node hosts the destination.
	// A stale assignment to a dead node is taken over, like SelectRoomNode does.
	node, err := r.router.GetNodeForRoom(ctx, destName)
	if err == nil && node != nil && selector.IsAvailable(node) && livekit.NodeID(node.Id) != r.currentNode.NodeID() {
		return nil, ErrForwardCrossNode
	} else if err != nil && !errors.Is(err, routing.ErrNotFound) {
		return nil, err
	}

	// honours room.auto_create: without it the destination must already exist. The psrpc ctx carries no
	// caller grants, so a roomCreate grant on the API token does not lift that requirement here.
	if err := r.roomAllocator.ValidateCreateRoom(ctx, destName); err != nil {
		return nil, err
	}
	if err := r.router.SetNodeForRoom(ctx, destName, r.currentNode.NodeID()); err != nil {
		return nil, err
	}

	req := &livekit.CreateRoomRequest{Name: string(destName)}
	// the psrpc ctx carries no API key either, inherit the source room's so webhooks route the same way
	if apiKey := srcRoom.Internal().GetTags()[RoomAPIKeyTag]; apiKey != "" {
		req.Tags = map[string]string{RoomAPIKeyTag: apiKey}
	}
	return r.getOrCreateRoom(ctx, req)
}

// forwardedSourceForReq resolves the source participant of a forward addressed to (room, identity).
func (r *RoomManager) forwardedSourceForReq(ctx context.Context, req participantReq) (*rtc.Room, types.LocalParticipant, error) {
	room := r.GetRoom(ctx, livekit.RoomName(req.GetRoom()))
	if room == nil {
		return nil, nil, ErrRoomNotFound
	}
	src := room.GetForwardedParticipant(livekit.ParticipantIdentity(req.GetIdentity()))
	if src == nil {
		return nil, nil, ErrParticipantNotFound
	}
	return room, src, nil
}

// removeForwardedParticipant ends the forward addressed by req; this is how a forward is stopped explicitly.
func (r *RoomManager) removeForwardedParticipant(ctx context.Context, req participantReq) (*livekit.RemoveParticipantResponse, error) {
	room, src, err := r.forwardedSourceForReq(ctx, req)
	if err != nil {
		return nil, err
	}
	if !room.RemoveForwardedParticipant(src.Identity()) {
		return nil, ErrParticipantNotFound
	}
	src.GetLogger().Infow("removing forwarded participant", "destinationRoom", room.Name())
	return &livekit.RemoveParticipantResponse{}, nil
}

// forwardAwareErr turns a not-found error into ErrForwardedParticipantReadOnly when req addresses a forward.
func (r *RoomManager) forwardAwareErr(ctx context.Context, req participantReq, err error) error {
	if errors.Is(err, ErrParticipantNotFound) {
		if _, _, ferr := r.forwardedSourceForReq(ctx, req); ferr == nil {
			return ErrForwardedParticipantReadOnly
		}
	}
	return err
}
