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
  SnapshotHistory (..),
  StoredSnapshot,
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
  snapshotHistory,
  snapshotSchemaVersion,
  snapshotStreamVersion,
  snapshotEntity,
  streamAppendEvents,
  streamAppendExpectedVersion,
  streamAppendIdentity,
) where

import EventSorcery.Aggregate
import EventSorcery.Job.Internal (
  AttemptCount (..),
  JobLifecycleEvent (..),
  JobRecord (..),
  JobStatus (..),
  LeaseToken (..),
  encodeStoredJob,
  jobEventAppend,
 )
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
loadEntity (Store backend _ _) key = fmap (fmap fst) (loadCurrent backend key)


snapshotEntity
  :: forall backend entity
   . (EventSourced entity, EventStore backend)
  => Store backend entity
  -> StreamKey entity
  -> SnapshotHistory
  -> IO (Either (StoreError backend entity) (Maybe entity))
snapshotEntity (Store backend _ _) key history = do
  loaded <- loadCurrent backend key
  case loaded of
    Left failure -> pure (Left failure)
    Right (Nothing, _) -> pure (Right Nothing)
    Right (Just _, NoStream) ->
      pure (Left (StoreSnapshotDecodeFailed (StreamVersion 0) impossibleState))
    Right (Just entity, At version) -> do
      stored <-
        storeSnapshot
          backend
          ( SnapshotWrite
              ( Unrestricted
                  ( streamIdentity key
                  , StoredSnapshot
                      version
                      (schemaVersion (Proxy @entity))
                      history
                      (encodeSnapshot entity)
                  )
              )
          )
      pure case stored of
        Left failure -> Left (StoreBackendFailed failure)
        Right () -> Right (Just entity)


snapshotStreamVersion :: StoredSnapshot -> StreamVersion
snapshotStreamVersion (StoredSnapshot version _ _ _) = version


snapshotSchemaVersion :: StoredSnapshot -> SchemaVersion
snapshotSchemaVersion (StoredSnapshot _ version _ _) = version


snapshotHistory :: StoredSnapshot -> SnapshotHistory
snapshotHistory (StoredSnapshot _ _ history _) = history


executeCommand
  :: (EventSourced entity, EventStore backend)
  => Store backend entity
  -> StreamKey entity
  -> Command entity
  -> IO (Either (StoreError backend entity) entity)
executeCommand (Store backend limits nextJobId) key command = do
  loaded <- loadCurrent backend key
  case loaded of
    Left failure -> pure (Left failure)
    Right (current, expected) -> case decide current of
      Left failure -> pure (Left (StoreCommandRejected failure))
      Right effect -> do
        interpreted <-
          interpretEffect
            backend
            nextJobId
            key
            expected
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


loadCurrent
  :: forall backend entity
   . (EventSourced entity, EventStore backend)
  => backend
  -> StreamKey entity
  -> IO
       ( Either
           (StoreError backend entity)
           (Maybe entity, ExpectedVersion)
       )
loadCurrent backend key = do
  loadedSnapshot <- loadSnapshot backend (streamIdentity key)
  case loadedSnapshot of
    Left failure -> pure (Left (StoreBackendFailed failure))
    Right Nothing -> replayFullStream backend key
    Right (Just snapshot) -> loadFromSnapshot backend key snapshot


loadFromSnapshot
  :: forall backend entity
   . (EventSourced entity, EventStore backend)
  => backend
  -> StreamKey entity
  -> StoredSnapshot
  -> IO
       ( Either
           (StoreError backend entity)
           (Maybe entity, ExpectedVersion)
       )
loadFromSnapshot backend key snapshot@(StoredSnapshot version storedSchema history payload)
  | storedSchema == expectedSchema = case decodeSnapshot @entity payload of
      Left failure -> pure (Left (StoreSnapshotDecodeFailed version failure))
      Right entity -> resumeSnapshot backend key version entity
  | history == RetainedHistory = do
      discarded <- discardSnapshot backend (streamIdentity key)
      case discarded of
        Left failure -> pure (Left (StoreBackendFailed failure))
        Right () -> replayFullStream backend key
  | otherwise =
      pure
        ( Left
            ( StoreSnapshotSchemaMismatch
                (snapshotHistory snapshot)
                expectedSchema
                storedSchema
            )
        )
  where
    expectedSchema = schemaVersion (Proxy @entity)


resumeSnapshot
  :: (EventSourced entity, EventStore backend)
  => backend
  -> StreamKey entity
  -> StreamVersion
  -> entity
  -> IO
       ( Either
           (StoreError backend entity)
           (Maybe entity, ExpectedVersion)
       )
resumeSnapshot backend key version entity = do
  loaded <- loadStreamAfter backend (streamIdentity key) version
  pure case loaded of
    Left failure -> Left (StoreBackendFailed failure)
    Right events -> do
      resumed <- first StoreReplayFailed (resume key version entity events)
      pure (Just resumed, versionAfter version events)


replayFullStream
  :: (EventSourced entity, EventStore backend)
  => backend
  -> StreamKey entity
  -> IO
       ( Either
           (StoreError backend entity)
           (Maybe entity, ExpectedVersion)
       )
replayFullStream backend key = do
  loaded <- loadStream backend (streamIdentity key)
  pure case loaded of
    Left failure -> Left (StoreBackendFailed failure)
    Right events -> do
      entity <- first StoreReplayFailed (replay key events)
      pure (entity, currentVersion events)


versionAfter :: StreamVersion -> [StoredEvent] -> ExpectedVersion
versionAfter version [] = At version
versionAfter _ events = case lastMay events of
  Nothing -> NoStream
  Just stored -> At (positionVersion stored.position)


positionVersion :: StreamPosition -> StreamVersion
positionVersion (StreamPosition position) = StreamVersion position


impossibleState :: DecodeCause
impossibleState = DecodeCause "entity exists without a stream version"


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
  jobEventAppend identifier NoStream JobEnqueuedEvent initialRecord
  where
    initialRecord =
      JobRecord
        (encodeStoredJob job)
        JobReady
        (LeaseToken 0)
        (AttemptCount 0)
