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
import EventSorcery.Delivery.Internal
import EventSorcery.Job.Internal
import EventSorcery.Projection.Internal
import EventSorcery.Reactor.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype MemoryStore = MemoryStore (TVar MemoryState)


data MemoryState = MemoryState
  { streams :: Map.Map StreamIdentity [StoredEvent]
  , eventJournal :: Seq.Seq StoredEnvelope
  , projections :: Map.Map ProjectionName ProjectionState
  , deliveryReceipts :: Set.Set DeliveryId
  , jobs :: Map.Map JobId JobRecord
  , reactors :: MemoryReactorState
  }


data MemoryReactorState
  = MemoryReactorState
      (Map.Map ReactorName EventOffset)
      (Map.Map DeliveryId OutboxEntry)


type MemoryError = Void


newMemoryStore :: IO MemoryStore
newMemoryStore =
  MemoryStore
    <$> newTVarIO
      MemoryState
        { streams = Map.empty
        , eventJournal = Seq.empty
        , projections = Map.empty
        , deliveryReceipts = Set.empty
        , jobs = Map.empty
        , reactors = MemoryReactorState Map.empty Map.empty
        }


instance EventStore MemoryStore where
  type BackendError MemoryStore = MemoryError


  loadStream (MemoryStore memoryState) streamIdentity = do
    current <- readTVarIO memoryState
    pure (Right (loadMemoryStream streamIdentity current))


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
  Map.findWithDefault [] streamIdentity memoryState.streams


isAfter :: EventOffset -> StoredEnvelope -> Bool
isAfter offset (StoredEnvelope storedOffset _ _) = storedOffset > offset


validateExpectedVersions
  :: Map.Map StreamIdentity [StoredEvent]
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
        actual = currentVersion (Map.findWithDefault [] streamIdentity streams)


applyAppend
  :: MemoryState
  -> StreamAppend
  -> MemoryState
applyAppend memoryState append =
  memoryState
    { streams =
        Map.insert streamIdentity (existing <> storedEvents) memoryState.streams
    , eventJournal = memoryState.eventJournal <> Seq.fromList envelopes
    }
  where
    streamIdentity = streamAppendIdentity append
    existing = Map.findWithDefault [] streamIdentity memoryState.streams
    firstPosition = case currentVersion existing of
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
