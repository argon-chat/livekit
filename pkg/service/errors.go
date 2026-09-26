// Copyright 2023 LiveKit, Inc.
// Modifications Copyright 2026 Argon Inc. LLC
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
	"github.com/livekit/psrpc"
)

var (
	ErrEgressNotFound                   = psrpc.NewErrorf(psrpc.NotFound, "egress does not exist")
	ErrEgressNotConnected               = psrpc.NewErrorf(psrpc.Internal, "egress not connected (redis required)")
	ErrIdentityEmpty                    = psrpc.NewErrorf(psrpc.InvalidArgument, "identity cannot be empty")
	ErrParticipantSidEmpty              = psrpc.NewErrorf(psrpc.InvalidArgument, "participant sid cannot be empty")
	ErrIngressNotConnected              = psrpc.NewErrorf(psrpc.Internal, "ingress not connected (redis required)")
	ErrIngressNotFound                  = psrpc.NewErrorf(psrpc.NotFound, "ingress does not exist")
	ErrIngressNonReusable               = psrpc.NewErrorf(psrpc.InvalidArgument, "ingress is not reusable and cannot be modified")
	ErrNameExceedsLimits                = psrpc.NewErrorf(psrpc.InvalidArgument, "name length exceeds limits")
	ErrMetadataExceedsLimits            = psrpc.NewErrorf(psrpc.InvalidArgument, "metadata size exceeds limits")
	ErrAttributeExceedsLimits           = psrpc.NewErrorf(psrpc.InvalidArgument, "attribute size exceeds limits")
	ErrNoRoomName                       = psrpc.NewErrorf(psrpc.InvalidArgument, "no room name")
	ErrRoomNameExceedsLimits            = psrpc.NewErrorf(psrpc.InvalidArgument, "room name length exceeds limits")
	ErrParticipantIdentityExceedsLimits = psrpc.NewErrorf(psrpc.InvalidArgument, "participant identity length exceeds limits")
	ErrDestinationSameAsSourceRoom      = psrpc.NewErrorf(psrpc.InvalidArgument, "destination room cannot be the same as source room")
	ErrOperationFailed                  = psrpc.NewErrorf(psrpc.Internal, "operation cannot be completed")
	ErrParticipantNotFound              = psrpc.NewErrorf(psrpc.NotFound, "participant does not exist")
	ErrRoomNotFound                     = psrpc.NewErrorf(psrpc.NotFound, "requested room does not exist")
	ErrRoomLockFailed                   = psrpc.NewErrorf(psrpc.Internal, "could not lock room")
	ErrRoomUnlockFailed                 = psrpc.NewErrorf(psrpc.Internal, "could not unlock room, lock token does not match")
	ErrRemoteUnmuteNoteEnabled          = psrpc.NewErrorf(psrpc.FailedPrecondition, "remote unmute not enabled")
	ErrTrackNotFound                    = psrpc.NewErrorf(psrpc.NotFound, "track is not found")
	ErrWebHookMissingAPIKey             = psrpc.NewErrorf(psrpc.InvalidArgument, "api_key is required to use webhooks")
	ErrWebHookRouteUnknownAPIKey        = psrpc.NewErrorf(psrpc.InvalidArgument, "webhook route api_key is not one of the configured keys")
	ErrWebHookRouteDuplicateAPIKey      = psrpc.NewErrorf(psrpc.InvalidArgument, "webhook route api_key is used by more than one route")
	ErrWebHookRouteMissingURLs          = psrpc.NewErrorf(psrpc.InvalidArgument, "webhook route requires at least one url")
	ErrSIPNotConnected                  = psrpc.NewErrorf(psrpc.Internal, "sip not connected (redis required)")
	ErrSIPTrunkNotFound                 = psrpc.NewErrorf(psrpc.NotFound, "requested sip trunk does not exist")
	ErrSIPDispatchRuleNotFound          = psrpc.NewErrorf(psrpc.NotFound, "requested sip dispatch rule does not exist")
	ErrSIPParticipantNotFound           = psrpc.NewErrorf(psrpc.NotFound, "requested sip participant does not exist")
	ErrInvalidMessageType               = psrpc.NewErrorf(psrpc.Internal, "invalid message type")
	ErrNoConnectRequest                 = psrpc.NewErrorf(psrpc.InvalidArgument, "no connect request")
	ErrNoConnectResponse                = psrpc.NewErrorf(psrpc.InvalidArgument, "no connect response")
	ErrDestinationIdentityRequired      = psrpc.NewErrorf(psrpc.InvalidArgument, "destination identity is required")

	// Argon: participant forwarding
	ErrForwardCrossNode             = psrpc.NewErrorf(psrpc.FailedPrecondition, "destination room is hosted on another node, forwarding is same-node only")
	ErrForwardIdentityInUse         = psrpc.NewErrorf(psrpc.AlreadyExists, "a participant with the same identity is already in the destination room")
	ErrForwardedParticipantReadOnly = psrpc.NewErrorf(psrpc.FailedPrecondition, "participant is forwarded into this room, update it in its source room")
	ErrForwardHiddenParticipant     = psrpc.NewErrorf(psrpc.InvalidArgument, "hidden participants cannot be forwarded")

	// Argon: participant move
	ErrMoveCrossNode     = psrpc.NewErrorf(psrpc.FailedPrecondition, "destination room is hosted on another node, moving is same-node only")
	ErrMoveIdentityInUse = psrpc.NewErrorf(psrpc.AlreadyExists, "a participant with the same identity is already in the destination room")
	ErrMoveNoSigningKey  = psrpc.NewErrorf(psrpc.FailedPrecondition, "no configured API key to sign the move token with")
)
