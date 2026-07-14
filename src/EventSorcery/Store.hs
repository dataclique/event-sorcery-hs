module EventSorcery.Store (
  BatchLimit,
  CommitBatch,
  CommitError (..),
  CommitLimitViolation (..),
  CommitLimits,
  EventOffset (..),
  EventStore (..),
  PayloadLimit,
  ProposedEvent (..),
  StreamAppend,
  StreamIdentity (..),
  StoredEnvelope (..),
  Store,
  StoreConflict (..),
  StoreError (..),
  Unrestricted (..),
  appendEvents,
  commitBatch,
  consumeCommitBatch,
  mkBatchLimit,
  mkCommitLimits,
  mkPayloadLimit,
  mkStore,
  executeCommand,
  loadEntity,
  streamAppendEvents,
  streamAppendExpectedVersion,
  streamAppendIdentity,
) where

import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LazyByteString
import EventSorcery.Aggregate
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


mkPayloadLimit :: Word64 -> Maybe PayloadLimit
mkPayloadLimit 0 = Nothing
mkPayloadLimit value = Just (PayloadLimit value)


mkBatchLimit :: Word64 -> Maybe BatchLimit
mkBatchLimit 0 = Nothing
mkBatchLimit value = Just (BatchLimit value)


mkCommitLimits :: PayloadLimit -> BatchLimit -> CommitLimits
mkCommitLimits = CommitLimits


mkStore :: backend -> CommitLimits -> IO JobId -> Store backend entity
mkStore = Store


loadEntity
  :: (EventSourced entity, EventStore backend)
  => Store backend entity
  -> StreamKey entity
  -> IO (Either (StoreError backend entity) (Maybe entity))
loadEntity (Store backend _ _) key = do
  loaded <- loadStream backend (streamIdentity key)
  pure case loaded of
    Left failure -> Left (StoreBackendFailed failure)
    Right events -> first StoreReplayFailed (replay key events)


executeCommand
  :: (EventSourced entity, EventStore backend)
  => Store backend entity
  -> StreamKey entity
  -> Command entity
  -> IO (Either (StoreError backend entity) entity)
executeCommand (Store backend limits nextJobId) key command = do
  loaded <- loadStream backend (streamIdentity key)
  case loaded of
    Left failure -> pure (Left (StoreBackendFailed failure))
    Right storedEvents -> case replay key storedEvents of
      Left failure -> pure (Left (StoreReplayFailed failure))
      Right current -> case decide current of
        Left failure -> pure (Left (StoreCommandRejected failure))
        Right effect -> do
          interpreted <-
            interpretEffect
              backend
              nextJobId
              key
              (currentVersion storedEvents)
              current
              effect
          case interpreted of
            Left failure -> pure (Left failure)
            Right (next, appends) ->
              commitEffect backend limits key next appends
  where
    decide current = case current of
      Nothing -> initialize command
      Just entity -> transition entity command


interpretEffect
  :: forall backend entity
   . EventSourced entity
  => backend
  -> IO JobId
  -> StreamKey entity
  -> ExpectedVersion
  -> Maybe entity
  -> Effect entity
  -> IO
       ( Either
           (StoreError backend entity)
           (entity, NonEmpty StreamAppend)
       )
interpretEffect _ _ key expected current (Events events) =
  pure do
    next <- first StoreDecisionRejected (applyEvents current events)
    pure
      ( next
      , appendEvents key expected events :| []
      )
interpretEffect _ nextJobId key expected current (Dispatch job) = do
  identifier <- nextJobId
  let intent = injectDispatchIntent (dispatchIntent identifier job)
  pure do
    next <- first StoreDecisionRejected (applyEvents current (intent :| []))
    pure
      ( next
      , appendEvents key expected (intent :| [])
          :| [frameworkJobAppend identifier job]
      )


commitEffect
  :: EventStore backend
  => backend
  -> CommitLimits
  -> StreamKey entity
  -> entity
  -> NonEmpty StreamAppend
  -> IO (Either (StoreError backend entity) entity)
commitEffect backend limits key next appends =
  case commitBatch limits appends of
    Left failure -> pure (Left (StoreCommitLimitExceeded failure))
    Right batch -> do
      committed <- commit backend batch
      pure case committed of
        Left (ConcurrencyConflict conflictingStream expected actual) ->
          Left
            ( StoreConcurrencyConflict
                (classifyConflict key conflictingStream)
                expected
                actual
            )
        Left (BackendFailed failure) -> Left (StoreBackendFailed failure)
        Right () -> Right next


applyEvents
  :: EventSourced entity
  => Maybe entity
  -> NonEmpty (Event entity)
  -> Either (ApplyError entity) entity
applyEvents current events = case current of
  Nothing -> do
    let firstEvent :| remaining = events
    initial <- originate firstEvent
    foldM evolve initial remaining
  Just entity -> foldM evolve entity events


streamIdentity :: StreamKey entity -> StreamIdentity
streamIdentity key = uncurry StreamIdentity (streamKeyParts key)


classifyConflict :: StreamKey entity -> StreamIdentity -> StoreConflict entity
classifyConflict key conflictingStream
  | conflictingStream == streamIdentity key = EntityStreamConflict key
classifyConflict _ (StreamIdentity "job" identifier) =
  maybe UnknownStreamConflict JobStreamConflict (mkJobId identifier)
classifyConflict _ _ = UnknownStreamConflict


frameworkJobAppend :: Job job => JobId -> job -> StreamAppend
frameworkJobAppend identifier job =
  StreamAppend
    jobStreamIdentity
    NoStream
    ( ProposedEvent
        (EventMetadata "job" encodedId "enqueued" (EventVersion 1))
        (encodeFrameworkJob job)
        :| []
    )
  where
    encodedId = jobIdText identifier
    jobStreamIdentity = StreamIdentity "job" encodedId


encodeFrameworkJob :: forall job. Job job => job -> ByteString
encodeFrameworkJob job =
  LazyByteString.toStrict
    ( Builder.toLazyByteString
        ( Builder.word64BE (fromIntegral (ByteString.length jobTypeBytes))
            <> Builder.byteString jobTypeBytes
            <> Builder.byteString (encodeJob job)
        )
    )
  where
    jobTypeBytes = encodeUtf8 (jobType (Proxy @job))
