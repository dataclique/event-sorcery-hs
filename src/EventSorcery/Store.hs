module EventSorcery.Store (
  BatchLimit,
  CommitBatch,
  CommitError (..),
  CommitLimitViolation (..),
  CommitLimits,
  EventStore (..),
  PayloadLimit,
  ProposedEvent (..),
  StreamAppend,
  StreamIdentity (..),
  Unrestricted (..),
  appendEvents,
  commitBatch,
  consumeCommitBatch,
  mkBatchLimit,
  mkCommitLimits,
  mkPayloadLimit,
  streamAppendEvents,
  streamAppendExpectedVersion,
  streamAppendIdentity,
) where

import EventSorcery.Store.Internal
import Protolude


mkPayloadLimit :: Word64 -> Maybe PayloadLimit
mkPayloadLimit 0 = Nothing
mkPayloadLimit value = Just (PayloadLimit value)


mkBatchLimit :: Word64 -> Maybe BatchLimit
mkBatchLimit 0 = Nothing
mkBatchLimit value = Just (BatchLimit value)


mkCommitLimits :: PayloadLimit -> BatchLimit -> CommitLimits
mkCommitLimits = CommitLimits
