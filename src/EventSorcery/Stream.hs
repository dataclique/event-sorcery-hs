module EventSorcery.Stream (
  ActualSequence (..),
  EventMetadata (..),
  ExpectedSequence (..),
  ExpectedVersion (..),
  MetadataMismatch (..),
  ReplayError (..),
  StoredEvent (..),
  StreamKey,
  StreamPosition (..),
  StreamVersion (..),
  replay,
  streamKey,
  streamKeyParts,
) where

import EventSorcery.Aggregate
import Protolude


newtype StreamVersion = StreamVersion Word64
  deriving stock (Eq, Ord, Show)


newtype StreamPosition = StreamPosition Word64
  deriving stock (Eq, Ord, Show)


newtype ExpectedSequence = ExpectedSequence StreamPosition
  deriving stock (Eq, Show)


newtype ActualSequence = ActualSequence StreamPosition
  deriving stock (Eq, Show)


data ExpectedVersion
  = NoStream
  | At StreamVersion
  deriving stock (Eq, Show)


data StreamKey entity = StreamKey Text Text
  deriving stock (Eq, Show)


data EventMetadata = EventMetadata
  { aggregateType :: Text
  , aggregateId :: Text
  , eventType :: Text
  , eventVersion :: EventVersion
  }
  deriving stock (Eq, Show)


data StoredEvent = StoredEvent
  { position :: StreamPosition
  , metadata :: EventMetadata
  , payload :: ByteString
  }
  deriving stock (Eq, Show)


data MetadataMismatch
  = AggregateTypeMismatch Text Text
  | AggregateIdMismatch Text Text
  | EventTypeMismatch Text Text
  | EventVersionMismatch EventVersion EventVersion
  deriving stock (Eq, Show)


data ReplayError entity
  = EventDecodeFailed StreamPosition DecodeCause
  | EventMetadataMismatch StreamPosition MetadataMismatch
  | EventSequenceMismatch ExpectedSequence ActualSequence
  | EventApplicationFailed StreamPosition (ApplyError entity)


deriving stock instance Eq (ApplyError entity) => Eq (ReplayError entity)


deriving stock instance Show (ApplyError entity) => Show (ReplayError entity)


streamKey
  :: forall entity. EventSourced entity => EntityId entity -> StreamKey entity
streamKey identifier =
  StreamKey
    (EventSorcery.Aggregate.aggregateType (Proxy @entity))
    (encodeEntityId identifier)


streamKeyParts :: StreamKey entity -> (Text, Text)
streamKeyParts (StreamKey aggregateName identifier) = (aggregateName, identifier)


replay
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> [StoredEvent]
  -> Either (ReplayError entity) (Maybe entity)
replay key events =
  fst <$> foldM (replayEvent key) (Nothing, StreamPosition 1) events


replayEvent
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> (Maybe entity, StreamPosition)
  -> StoredEvent
  -> Either (ReplayError entity) (Maybe entity, StreamPosition)
replayEvent key (currentState, expectedPosition) stored = do
  validateSequence expectedPosition stored
  validateStreamMetadata key stored
  event <-
    first (EventDecodeFailed stored.position) (decodeEvent @entity stored.payload)
  validateEventMetadata stored event
  nextState <-
    first (EventApplicationFailed stored.position) case currentState of
      Nothing -> originate event
      Just current -> evolve current event
  pure (Just nextState, nextPosition expectedPosition)


validateSequence
  :: StreamPosition -> StoredEvent -> Either (ReplayError entity) ()
validateSequence expected stored
  | stored.position == expected = Right ()
  | otherwise =
      Left
        ( EventSequenceMismatch
            (ExpectedSequence expected)
            (ActualSequence stored.position)
        )


validateStreamMetadata
  :: StreamKey entity -> StoredEvent -> Either (ReplayError entity) ()
validateStreamMetadata (StreamKey expectedType expectedId) stored
  | stored.metadata.aggregateType /= expectedType =
      mismatch
        (AggregateTypeMismatch expectedType stored.metadata.aggregateType)
  | stored.metadata.aggregateId /= expectedId =
      mismatch
        (AggregateIdMismatch expectedId stored.metadata.aggregateId)
  | otherwise = Right ()
  where
    mismatch = Left . EventMetadataMismatch stored.position


validateEventMetadata
  :: EventSourced entity
  => StoredEvent
  -> Event entity
  -> Either (ReplayError entity) ()
validateEventMetadata stored event
  | stored.metadata.eventType /= expectedType =
      mismatch (EventTypeMismatch expectedType stored.metadata.eventType)
  | stored.metadata.eventVersion /= expectedVersion =
      mismatch
        (EventVersionMismatch expectedVersion stored.metadata.eventVersion)
  | otherwise = Right ()
  where
    expectedType = EventSorcery.Aggregate.eventType event
    expectedVersion = EventSorcery.Aggregate.eventVersion event
    mismatch = Left . EventMetadataMismatch stored.position


nextPosition :: StreamPosition -> StreamPosition
nextPosition (StreamPosition position) = StreamPosition (position + 1)
