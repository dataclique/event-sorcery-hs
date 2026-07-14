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
import EventSorcery.Projection.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype MemoryStore = MemoryStore (TVar MemoryState)


data MemoryState
  = MemoryState
      (Map.Map StreamIdentity [StoredEvent])
      (Seq.Seq StoredEnvelope)
      (Map.Map ProjectionName ProjectionState)
      (Set.Set DeliveryId)


type MemoryError = Void


newMemoryStore :: IO MemoryStore
newMemoryStore =
  MemoryStore
    <$> newTVarIO (MemoryState Map.empty Seq.empty Map.empty Set.empty)


instance EventStore MemoryStore where
  type BackendError MemoryStore = MemoryError


  loadStream (MemoryStore streams) streamIdentity =
    Right . loadMemoryStream streamIdentity <$> readTVarIO streams


  streamEventsAfter (MemoryStore memoryState) offset = do
    MemoryState _ journal _ _ <- liftIO (readTVarIO memoryState)
    Conduit.yieldMany (filter (isAfter offset) (toList journal))


  commit = commitMemory


instance ProjectionStore MemoryStore where
  loadProjection (MemoryStore memoryState) name = do
    MemoryState _ _ projections _ <- readTVarIO memoryState
    pure (Right (Map.lookup name projections))


  advanceProjection (MemoryStore memoryState) update =
    case consumeProjectionUpdate update of
      Unrestricted (name, offset, view) -> atomically do
        MemoryState streams journal projections receipts <- readTVar memoryState
        case decideProjectionAdvance
          name
          offset
          view
          (Map.lookup name projections) of
          Left failure -> pure (Left failure)
          Right (advance, next) -> do
            let updated = case next of
                  Nothing -> projections
                  Just projectionState ->
                    Map.insert name projectionState projections
            writeTVar
              memoryState
              (MemoryState streams journal updated receipts)
            pure (Right advance)


  resetProjection (MemoryStore memoryState) name = atomically do
    MemoryState streams journal projections receipts <- readTVar memoryState
    writeTVar
      memoryState
      (MemoryState streams journal (Map.delete name projections) receipts)
    pure (Right ())


instance DeliveryStore MemoryStore where
  commitDelivery (MemoryStore memoryState) delivery batch =
    case consumeCommitBatch batch of
      Unrestricted appends -> atomically do
        current@(MemoryState streams _ _ receipts) <- readTVar memoryState
        if Set.member delivery receipts
          then pure (Right DeliveryAlreadyApplied)
          else case validateExpectedVersions streams appends of
            Left conflict -> pure (Left conflict)
            Right () -> do
              let committed = foldl' applyAppend current appends
              writeTVar memoryState (recordDelivery delivery committed)
              pure (Right DeliveryApplied)


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
  memoryState@(MemoryState stored _ _ _) <- readTVar streams
  case validateExpectedVersions stored appends of
    Left conflict -> pure (Left conflict)
    Right () -> do
      writeTVar streams (foldl' applyAppend memoryState appends)
      pure (Right ())


loadMemoryStream :: StreamIdentity -> MemoryState -> [StoredEvent]
loadMemoryStream streamIdentity (MemoryState streams _ _ _) =
  Map.findWithDefault [] streamIdentity streams


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
applyAppend (MemoryState streams journal projections receipts) append =
  MemoryState
    (Map.insert streamIdentity (existing <> storedEvents) streams)
    (journal <> Seq.fromList envelopes)
    projections
    receipts
  where
    streamIdentity = streamAppendIdentity append
    existing = Map.findWithDefault [] streamIdentity streams
    firstPosition = case currentVersion existing of
      NoStream -> 1
      At (StreamVersion version) -> version + 1
    storedEvents =
      zipWith
        toStored
        [firstPosition ..]
        (NonEmpty.toList (streamAppendEvents append))
    firstOffset = fromIntegral (Seq.length journal) + 1
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
recordDelivery delivery (MemoryState streams journal projections receipts) =
  MemoryState
    streams
    journal
    projections
    (Set.insert delivery receipts)
