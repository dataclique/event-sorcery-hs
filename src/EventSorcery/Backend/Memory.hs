module EventSorcery.Backend.Memory (
  MemoryError,
  MemoryStore,
  newMemoryStore,
) where

import Conduit qualified
import Control.Concurrent.STM (
  TVar,
  newTVarIO,
  readTVar,
  readTVarIO,
  writeTVar,
 )
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as Map
import Data.Sequence qualified as Seq
import Data.Set qualified as Set
import EventSorcery.Aggregate (SchemaVersion)
import EventSorcery.Delivery.Internal
import EventSorcery.Job.Internal
import EventSorcery.Projection.Internal
import EventSorcery.Reactor.Internal
import EventSorcery.Schema.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype MemoryStore = MemoryStore (TVar MemoryState)


data MemoryState = MemoryState
  { streams :: Map.Map StreamIdentity (Seq.Seq StoredEvent)
  , eventJournal :: Seq.Seq StoredEnvelope
  , snapshots :: Map.Map StreamIdentity StoredSnapshot
  , projections :: Map.Map ProjectionName ProjectionState
  , deliveryReceipts :: Set.Set DeliveryId
  , jobs :: Map.Map JobId JobRecord
  , reactors :: MemoryReactorState
  , schemas :: Map.Map SchemaTarget SchemaVersion
  }


data MemoryReactorState
  = MemoryReactorState
      (Map.Map ReactorName EventOffset)
      (Map.Map DeliveryId OutboxEntry)


data MemoryError = MemorySnapshotVersionInvalid
  deriving stock (Eq, Show)


newMemoryStore :: IO MemoryStore
newMemoryStore =
  MemoryStore
    <$> newTVarIO
      MemoryState
        { streams = Map.empty
        , eventJournal = Seq.empty
        , snapshots = Map.empty
        , projections = Map.empty
        , deliveryReceipts = Set.empty
        , jobs = Map.empty
        , reactors = MemoryReactorState Map.empty Map.empty
        , schemas = Map.empty
        }


instance EventStore MemoryStore where
  type BackendError MemoryStore = MemoryError


  loadStream (MemoryStore memoryState) streamIdentity = do
    current <- readTVarIO memoryState
    pure (Right (loadMemoryStream streamIdentity current))


  loadStreamAfter (MemoryStore memoryState) streamIdentity version = do
    current <- readTVarIO memoryState
    pure (Right (loadMemoryStreamAfter streamIdentity version current))


  loadSnapshot (MemoryStore memoryState) streamIdentity = do
    current <- readTVarIO memoryState
    pure (Right (Map.lookup streamIdentity current.snapshots))


  discardSnapshot (MemoryStore memoryState) streamIdentity = atomically do
    current <- readTVar memoryState
    writeTVar
      memoryState
      current {snapshots = Map.delete streamIdentity current.snapshots}
    pure (Right ())


  storeSnapshot = storeMemorySnapshot


  streamEventsAfter (MemoryStore memoryState) offset = do
    current <- liftIO (readTVarIO memoryState)
    Conduit.yieldMany (filter (isAfter offset) (toList current.eventJournal))


  commit = commitMemory


instance ProjectionStore MemoryStore where
  loadProjection (MemoryStore memoryState) name = do
    current <- readTVarIO memoryState
    pure (Right (Map.lookup name current.projections))


  advanceProjection (MemoryStore memoryState) update =
    case consumeProjectionUpdate update of
      Unrestricted (name, offset, view) -> atomically do
        current <- readTVar memoryState
        case decideProjectionAdvance
          name
          offset
          view
          (Map.lookup name current.projections) of
          Left failure -> pure (Left failure)
          Right (advance, next) -> do
            let updated = case next of
                  Nothing -> current.projections
                  Just projectionState ->
                    Map.insert name projectionState current.projections
            writeTVar memoryState current {projections = updated}
            pure (Right advance)


  resetProjection (MemoryStore memoryState) name = atomically do
    current <- readTVar memoryState
    writeTVar
      memoryState
      current {projections = Map.delete name current.projections}
    pure (Right ())


instance DeliveryStore MemoryStore where
  commitDelivery (MemoryStore memoryState) delivery batch =
    case consumeCommitBatch batch of
      Unrestricted appends -> atomically do
        current <- readTVar memoryState
        if Set.member delivery current.deliveryReceipts
          then pure (Right DeliveryAlreadyApplied)
          else case validateExpectedVersions current.streams appends of
            Left conflict -> pure (Left conflict)
            Right () -> do
              let committed = foldl' applyAppend current appends
              writeTVar memoryState (recordDelivery delivery committed)
              pure (Right DeliveryApplied)


instance JobStore MemoryStore where
  enqueueJob (MemoryStore memoryState) identifier payload = atomically do
    current <- readTVar memoryState
    case decideEnqueue identifier payload (Map.lookup identifier current.jobs) of
      Left failure -> pure (Left failure)
      Right (result, next) -> do
        traverse_ (writeJob memoryState current identifier) next
        pure (Right result)


  claimJob (MemoryStore memoryState) identifier window = atomically do
    current <- readTVar memoryState
    case decideClaim identifier window (Map.lookup identifier current.jobs) of
      Left failure -> pure (Left failure)
      Right (claim, next) -> do
        writeJob memoryState current identifier next
        pure (Right claim)


  acknowledgeJob (MemoryStore memoryState) identifier token = atomically do
    current <- readTVar memoryState
    case decideAcknowledge identifier token (Map.lookup identifier current.jobs) of
      Left failure -> pure (Left failure)
      Right next -> do
        writeJob memoryState current identifier next
        pure (Right ())


instance ReactorStore MemoryStore where
  loadReactorCheckpoint (MemoryStore memoryState) name = do
    current <- readTVarIO memoryState
    let MemoryReactorState checkpoints _ = current.reactors
    pure (Right (Map.lookup name checkpoints))


  loadOutboxEntry (MemoryStore memoryState) identifier = do
    current <- readTVarIO memoryState
    let MemoryReactorState _ outbox = current.reactors
    pure (Right (Map.lookup identifier outbox))


  advanceReactor (MemoryStore memoryState) update =
    case consumeReactorUpdate update of
      Unrestricted (name, offset, proposed) -> atomically do
        current <- readTVar memoryState
        let reactorState = current.reactors
        let MemoryReactorState checkpoints outbox = reactorState
            checkpoint = Map.lookup name checkpoints
            existing =
              proposed >>= \entry -> Map.lookup (outboxDeliveryId entry) outbox
        case decideReactorAdvance name offset proposed checkpoint existing of
          Left failure -> pure (Left failure)
          Right (result, nextCheckpoint, insertion) -> do
            let nextReactorState =
                  updateMemoryReactor
                    name
                    nextCheckpoint
                    insertion
                    reactorState
            writeTVar memoryState (setMemoryReactor nextReactorState current)
            pure (Right result)


instance SchemaStore MemoryStore where
  reconcileSchema
    (MemoryStore memoryState)
    (SchemaRegistration target requested) = atomically do
      current <- readTVar memoryState
      let (result, invalidation) =
            decideSchemaReconciliation
              requested
              (Map.lookup target current.schemas)
          reconciled =
            case invalidation of
              PreserveDerivedState -> current
              InvalidateDerivedState -> invalidateMemorySchema target current
          next =
            reconciled
              { schemas = Map.insert target requested reconciled.schemas
              }
      writeTVar memoryState next
      pure (Right result)


commitMemory
  :: MemoryStore
  -> CommitBatch
  %1 -> IO (Either (CommitError MemoryStore) ())
commitMemory store batch = case consumeCommitBatch batch of
  Unrestricted appends -> commitAppends store appends


commitAppends
  :: MemoryStore
  -> NonEmpty StreamAppend
  -> IO (Either (CommitError MemoryStore) ())
commitAppends (MemoryStore streams) appends = atomically do
  memoryState <- readTVar streams
  case validateExpectedVersions memoryState.streams appends of
    Left conflict -> pure (Left conflict)
    Right () -> do
      writeTVar streams (foldl' applyAppend memoryState appends)
      pure (Right ())


loadMemoryStream :: StreamIdentity -> MemoryState -> [StoredEvent]
loadMemoryStream streamIdentity memoryState =
  toList (loadMemoryEvents streamIdentity memoryState)


loadMemoryEvents :: StreamIdentity -> MemoryState -> Seq.Seq StoredEvent
loadMemoryEvents streamIdentity memoryState =
  Map.findWithDefault Seq.empty streamIdentity memoryState.streams


loadMemoryStreamAfter
  :: StreamIdentity -> StreamVersion -> MemoryState -> [StoredEvent]
loadMemoryStreamAfter streamIdentity (StreamVersion version) memoryState =
  toList
    ( Seq.drop
        (fromIntegral version)
        (loadMemoryEvents streamIdentity memoryState)
    )


storeMemorySnapshot
  :: MemoryStore
  -> SnapshotWrite
  %1 -> IO (Either MemoryError ())
storeMemorySnapshot (MemoryStore memoryState) write =
  case consumeSnapshotWrite write of
    Unrestricted (streamIdentity, snapshot) -> atomically do
      current <- readTVar memoryState
      let stream = loadMemoryEvents streamIdentity current
      if snapshotWithinStream snapshot stream
        then do
          let nextSnapshots =
                advanceSnapshot
                  streamIdentity
                  snapshot
                  current.snapshots
          writeTVar memoryState current {snapshots = nextSnapshots}
          pure (Right ())
        else pure (Left MemorySnapshotVersionInvalid)


snapshotWithinStream :: StoredSnapshot -> Seq.Seq StoredEvent -> Bool
snapshotWithinStream (StoredSnapshot version _ _ _) events =
  case memoryVersion events of
    NoStream -> False
    At actual -> version <= actual


advanceSnapshot
  :: StreamIdentity
  -> StoredSnapshot
  -> Map.Map StreamIdentity StoredSnapshot
  -> Map.Map StreamIdentity StoredSnapshot
advanceSnapshot streamIdentity proposed current =
  case Map.lookup streamIdentity current of
    Just existing
      | snapshotVersion existing > snapshotVersion proposed -> current
    _ -> Map.insert streamIdentity proposed current


snapshotVersion :: StoredSnapshot -> StreamVersion
snapshotVersion (StoredSnapshot version _ _ _) = version


isAfter :: EventOffset -> StoredEnvelope -> Bool
isAfter offset (StoredEnvelope storedOffset _ _) = storedOffset > offset


validateExpectedVersions
  :: Map.Map StreamIdentity (Seq.Seq StoredEvent)
  -> NonEmpty StreamAppend
  -> Either (CommitError MemoryStore) ()
validateExpectedVersions streams = traverse_ validate
  where
    validate append
      | expected == actual = Right ()
      | otherwise = Left (ConcurrencyConflict streamIdentity expected actual)
      where
        streamIdentity = streamAppendIdentity append
        expected = streamAppendExpectedVersion append
        actual = memoryVersion (Map.findWithDefault Seq.empty streamIdentity streams)


applyAppend
  :: MemoryState
  -> StreamAppend
  -> MemoryState
applyAppend memoryState append =
  memoryState
    { streams =
        Map.insert
          streamIdentity
          (existing <> Seq.fromList storedEvents)
          memoryState.streams
    , eventJournal = memoryState.eventJournal <> Seq.fromList envelopes
    }
  where
    streamIdentity = streamAppendIdentity append
    existing = Map.findWithDefault Seq.empty streamIdentity memoryState.streams
    firstPosition = case memoryVersion existing of
      NoStream -> 1
      At (StreamVersion version) -> version + 1
    storedEvents =
      zipWith
        toStored
        [firstPosition ..]
        (NonEmpty.toList (streamAppendEvents append))
    firstOffset = fromIntegral (Seq.length memoryState.eventJournal) + 1
    envelopes =
      zipWith
        ( \offset event ->
            StoredEnvelope (EventOffset offset) streamIdentity event
        )
        [firstOffset ..]
        storedEvents
    toStored position (ProposedEvent metadata payload) =
      StoredEvent (StreamPosition position) metadata payload


memoryVersion :: Seq.Seq StoredEvent -> ExpectedVersion
memoryVersion events = case Seq.viewr events of
  Seq.EmptyR -> NoStream
  _ Seq.:> stored -> At (streamPositionVersion stored.position)


streamPositionVersion :: StreamPosition -> StreamVersion
streamPositionVersion (StreamPosition position) = StreamVersion position


recordDelivery :: DeliveryId -> MemoryState -> MemoryState
recordDelivery delivery memoryState =
  memoryState
    { deliveryReceipts = Set.insert delivery memoryState.deliveryReceipts
    }


writeJob
  :: TVar MemoryState
  -> MemoryState
  -> JobId
  -> JobRecord
  -> STM ()
writeJob
  memoryState
  current
  identifier
  record =
    writeTVar
      memoryState
      current {jobs = Map.insert identifier record current.jobs}


updateMemoryReactor
  :: ReactorName
  -> Maybe EventOffset
  -> Maybe OutboxEntry
  -> MemoryReactorState
  -> MemoryReactorState
updateMemoryReactor
  name
  checkpoint
  insertion
  (MemoryReactorState checkpoints outbox) =
    MemoryReactorState
      (maybe checkpoints (\offset -> Map.insert name offset checkpoints) checkpoint)
      ( maybe
          outbox
          (\entry -> Map.insert (outboxDeliveryId entry) entry outbox)
          insertion
      )


setMemoryReactor :: MemoryReactorState -> MemoryState -> MemoryState
setMemoryReactor
  reactors
  memoryState =
    memoryState {reactors = reactors}


invalidateMemorySchema :: SchemaTarget -> MemoryState -> MemoryState
invalidateMemorySchema target memoryState = case target of
  AggregateSchema aggregateName ->
    memoryState
      { snapshots =
          Map.filterWithKey
            (snapshotOutsideAggregate aggregateName)
            memoryState.snapshots
      }
  ProjectionSchema name ->
    memoryState {projections = Map.delete name memoryState.projections}


snapshotOutsideAggregate
  :: Text -> StreamIdentity -> StoredSnapshot -> Bool
snapshotOutsideAggregate expected (StreamIdentity actual _) _ =
  actual /= expected
