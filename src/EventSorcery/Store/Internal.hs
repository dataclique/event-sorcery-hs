module EventSorcery.Store.Internal (
  BatchLimit (..),
  CommitBatch (..),
  CommitError (..),
  CommitLimitViolation (..),
  CommitLimits (..),
  EventOffset (..),
  EventStore (..),
  PayloadLimit (..),
  ProposedEvent (..),
  StreamAppend (..),
  StreamIdentity (..),
  StoredEnvelope (..),
  Unrestricted (..),
  appendEvents,
  commitBatch,
  consumeCommitBatch,
  currentVersion,
  proposedEvents,
  streamAppendEvents,
  streamAppendExpectedVersion,
  streamAppendIdentity,
) where

import Conduit (ConduitT)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty qualified as NonEmpty
import Data.Set qualified as Set
import EventSorcery.Aggregate
import EventSorcery.Stream
import Protolude


newtype PayloadLimit = PayloadLimit Word64
  deriving stock (Eq, Ord, Show)


newtype BatchLimit = BatchLimit Word64
  deriving stock (Eq, Ord, Show)


data CommitLimits = CommitLimits PayloadLimit BatchLimit
  deriving stock (Eq, Show)


data StreamIdentity = StreamIdentity Text Text
  deriving stock (Eq, Ord, Show)


newtype EventOffset = EventOffset Word64
  deriving stock (Eq, Ord, Show)


data StoredEnvelope = StoredEnvelope EventOffset StreamIdentity StoredEvent
  deriving stock (Eq, Show)


data ProposedEvent = ProposedEvent EventMetadata ByteString
  deriving stock (Eq, Show)


data StreamAppend
  = StreamAppend StreamIdentity ExpectedVersion (NonEmpty ProposedEvent)


data CommitBatch where
  CommitBatch :: Unrestricted (NonEmpty StreamAppend) %1 -> CommitBatch


data Unrestricted value where
  Unrestricted :: value -> Unrestricted value


data CommitLimitViolation
  = EventPayloadTooLarge StreamIdentity Word64 PayloadLimit
  | BatchEventLimitExceeded Word64 BatchLimit
  | DuplicateStreamInBatch StreamIdentity
  deriving stock (Eq, Show)


data CommitError backend
  = ConcurrencyConflict StreamIdentity ExpectedVersion ExpectedVersion
  | BackendFailed (BackendError backend)


deriving stock instance Eq (BackendError backend) => Eq (CommitError backend)


deriving stock instance
  Show (BackendError backend) => Show (CommitError backend)


class EventStore backend where
  type BackendError backend


  loadStream
    :: backend
    -> StreamIdentity
    -> IO (Either (BackendError backend) [StoredEvent])
  streamEventsAfter
    :: backend
    -> EventOffset
    -> ConduitT
         ()
         StoredEnvelope
         (ExceptT (BackendError backend) IO)
         ()
  streamEventsAfter _ _ = pure ()
  commit :: backend -> CommitBatch %1 -> IO (Either (CommitError backend) ())


appendEvents
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> ExpectedVersion
  -> NonEmpty (Event entity)
  -> StreamAppend
appendEvents key expected events =
  StreamAppend streamIdentity expected (proposedEvents key <$> events)
  where
    (aggregateName, identifier) = streamKeyParts key
    streamIdentity = StreamIdentity aggregateName identifier


proposedEvents
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> Event entity
  -> ProposedEvent
proposedEvents key event =
  ProposedEvent
    (EventMetadata aggregateName identifier eventName version)
    (encodeEvent @entity event)
  where
    (aggregateName, identifier) = streamKeyParts key
    eventName = EventSorcery.Aggregate.eventType event
    version = EventSorcery.Aggregate.eventVersion event


commitBatch
  :: CommitLimits
  -> NonEmpty StreamAppend
  -> Either CommitLimitViolation CommitBatch
commitBatch (CommitLimits payloadLimit batchLimit) appends = do
  validateBatchSize batchLimit appends
  validateDistinctStreams appends
  traverse_ (validateAppend payloadLimit) appends
  pure (CommitBatch (Unrestricted appends))


consumeCommitBatch
  :: CommitBatch %1 -> Unrestricted (NonEmpty StreamAppend)
consumeCommitBatch (CommitBatch appends) = appends


currentVersion :: [StoredEvent] -> ExpectedVersion
currentVersion events = case lastMay events of
  Nothing -> NoStream
  Just stored -> At (streamPositionVersion stored.position)


streamPositionVersion :: StreamPosition -> StreamVersion
streamPositionVersion (StreamPosition position) = StreamVersion position


streamAppendIdentity :: StreamAppend -> StreamIdentity
streamAppendIdentity (StreamAppend streamIdentity _ _) = streamIdentity


streamAppendExpectedVersion :: StreamAppend -> ExpectedVersion
streamAppendExpectedVersion (StreamAppend _ expected _) = expected


streamAppendEvents :: StreamAppend -> NonEmpty ProposedEvent
streamAppendEvents (StreamAppend _ _ events) = events


validateBatchSize
  :: BatchLimit
  -> NonEmpty StreamAppend
  -> Either CommitLimitViolation ()
validateBatchSize limit@(BatchLimit maximumEvents) appends
  | eventCount <= maximumEvents = Right ()
  | otherwise = Left (BatchEventLimitExceeded eventCount limit)
  where
    eventCount =
      fromIntegral (sum (NonEmpty.length . streamAppendEvents <$> appends))


validateDistinctStreams
  :: NonEmpty StreamAppend -> Either CommitLimitViolation ()
validateDistinctStreams = go Set.empty . NonEmpty.toList
  where
    go _ [] = Right ()
    go seen (append : rest)
      | Set.member streamIdentity seen =
          Left (DuplicateStreamInBatch streamIdentity)
      | otherwise = go (Set.insert streamIdentity seen) rest
      where
        streamIdentity = streamAppendIdentity append


validateAppend :: PayloadLimit -> StreamAppend -> Either CommitLimitViolation ()
validateAppend limit append = traverse_ validate (streamAppendEvents append)
  where
    validate (ProposedEvent _ payload)
      | payloadSize <= maximumSize = Right ()
      | otherwise =
          Left (EventPayloadTooLarge (streamAppendIdentity append) payloadSize limit)
      where
        payloadSize = fromIntegral . ByteString.length $ payload
    PayloadLimit maximumSize = limit
