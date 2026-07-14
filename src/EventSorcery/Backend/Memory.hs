module EventSorcery.Backend.Memory (
  MemoryError,
  MemoryStore,
  newMemoryStore,
) where

import Control.Concurrent.STM (
  TVar,
  newTVarIO,
  readTVar,
  readTVarIO,
  writeTVar,
 )
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as Map
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype MemoryStore = MemoryStore (TVar (Map.Map StreamIdentity [StoredEvent]))


type MemoryError = Void


newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO Map.empty


instance EventStore MemoryStore where
  type BackendError MemoryStore = MemoryError


  loadStream (MemoryStore streams) streamIdentity =
    Right . Map.findWithDefault [] streamIdentity <$> readTVarIO streams


  commit = commitMemory


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
  stored <- readTVar streams
  case validateExpectedVersions stored appends of
    Left conflict -> pure (Left conflict)
    Right () -> do
      writeTVar streams (foldl' applyAppend stored appends)
      pure (Right ())


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
  :: Map.Map StreamIdentity [StoredEvent]
  -> StreamAppend
  -> Map.Map StreamIdentity [StoredEvent]
applyAppend streams append =
  Map.insert streamIdentity (existing <> storedEvents) streams
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
    toStored position (ProposedEvent metadata payload) =
      StoredEvent (StreamPosition position) metadata payload
