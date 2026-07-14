module Main (main) where

import Conduit (foldlC, runConduit, (.|))
import Criterion.Main
import Data.Aeson qualified as Aeson
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LazyByteString
import EventSorcery
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Backend.Memory
import EventSorcery.Backend.SQLite
import Protolude


newtype AccountId = AccountId Text


newtype Account = Account Word64


data AccountCommand = Open


data AccountEvent
  = Opened Word64
  | Deposited Word64
  deriving stock (Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data AccountCommandError = AccountCommandError


data AccountApplyError = AccountApplyError


newtype MemoryFixture = MemoryFixture MemoryStore


newtype SQLiteFixture = SQLiteFixture SQLiteStore


data SQLiteCommitFixture = SQLiteCommitFixture SQLiteStore CommitBatch


data MemoryJobFixture = MemoryJobFixture MemoryStore [LeaseWindow]


data SQLiteJobFixture = SQLiteJobFixture SQLiteStore [LeaseWindow]


instance NFData MemoryFixture where
  rnf (MemoryFixture store) = store `seq` ()


instance NFData SQLiteFixture where
  rnf (SQLiteFixture store) = store `seq` ()


instance NFData SQLiteCommitFixture where
  rnf (SQLiteCommitFixture store batch) = store `seq` batch `seq` ()


instance NFData MemoryJobFixture where
  rnf (MemoryJobFixture store windows) = store `seq` rnf windows


instance NFData SQLiteJobFixture where
  rnf (SQLiteJobFixture store windows) = store `seq` rnf windows


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[]


  aggregateType _ = "benchmark-account"
  encodeEntityId (AccountId identifier) = identifier
  eventType (Opened _) = "opened"
  eventType (Deposited _) = "deposited"
  eventVersion _ = EventVersion 1
  schemaVersion _ = SchemaVersion 1
  encodeEvent = LazyByteString.toStrict . Aeson.encode
  decodeEvent =
    first (const (DecodeCause "invalid benchmark event"))
      . Aeson.eitherDecodeStrict'
  originate (Opened amount) = Right (Account amount)
  originate (Deposited _) = Left AccountApplyError
  evolve (Account balance) (Deposited amount) =
    Right (Account (balance + amount))
  evolve account (Opened _) = Right account
  initialize Open = Right (Events (Opened 0 :| []))
  transition _ Open = Left AccountCommandError


main :: IO ()
main = do
  let replayInput = replayFixture eventCount
  evaluate (forceEvents replayInput)
  defaultMain
    [ bgroup
        "replay"
        [bench "10000 events" (nf replayBalance replayInput)]
    , bgroup
        "catch-up"
        [ env setupMemoryFixture $
            bench "memory/10000 events" . nfIO . consumeMemory
        , envWithCleanup setupSQLiteFixture closeSQLiteFixture $
            bench "sqlite/10000 events" . nfIO . consumeSQLite
        ]
    , bgroup
        "projection"
        [ bench "memory/advance" $
            perRunEnv setupEmptyMemoryFixture advanceMemoryProjection
        ]
    , bgroup
        "commit"
        [ bench "sqlite/100 events" $
            perRunEnvWithCleanup
              setupSQLiteCommitFixture
              closeSQLiteCommitFixture
              commitSQLiteFixture
        ]
    , bgroup
        "jobs"
        [ bench "memory/1000 lease claims" $
            perRunEnv setupMemoryJobFixture claimMemoryJob
        , bench "sqlite/1000 lease claims" $
            perRunEnvWithCleanup
              setupSQLiteJobFixture
              closeSQLiteJobFixture
              claimSQLiteJob
        ]
    ]


eventCount :: Word64
eventCount = 10000


replayFixture :: Word64 -> [StoredEvent]
replayFixture count =
  stored 1 (Opened 0)
    : ((\position -> stored position (Deposited 1)) <$> [2 .. count])


replayBalance :: [StoredEvent] -> Word64
replayBalance events = case replay accountKey events of
  Right (Just (Account balance)) -> balance
  _ -> panic "benchmark replay failed"


forceEvents :: [StoredEvent] -> ()
forceEvents = foldl' forceEvent ()
  where
    forceEvent forced event =
      forced `seq`
        event.position `seq`
          event.metadata.aggregateType `seq`
            event.metadata.aggregateId `seq`
              event.metadata.eventType `seq`
                event.metadata.eventVersion `seq`
                  ByteString.length event.payload `seq`
                    ()


setupMemoryFixture :: IO MemoryFixture
setupMemoryFixture = do
  store <- newMemoryStore
  batch <- benchmarkBatch eventCount
  commit store batch >>= requireRight
  pure (MemoryFixture store)


setupEmptyMemoryFixture :: IO MemoryFixture
setupEmptyMemoryFixture = MemoryFixture <$> newMemoryStore


setupSQLiteFixture :: IO SQLiteFixture
setupSQLiteFixture = do
  store <- openBenchmarkSQLite
  batch <- benchmarkBatch eventCount
  commit store batch >>= requireRight
  pure (SQLiteFixture store)


closeSQLiteFixture :: SQLiteFixture -> IO ()
closeSQLiteFixture (SQLiteFixture store) = closeSQLiteStore store


setupSQLiteCommitFixture :: IO SQLiteCommitFixture
setupSQLiteCommitFixture = do
  store <- openBenchmarkSQLite
  batch <- benchmarkBatch 100
  pure (SQLiteCommitFixture store batch)


setupMemoryJobFixture :: IO MemoryJobFixture
setupMemoryJobFixture = do
  store <- newMemoryStore
  enqueueJob store benchmarkJobId "payload" >>= requireJobEnqueued
  pure (MemoryJobFixture store benchmarkLeaseWindows)


setupSQLiteJobFixture :: IO SQLiteJobFixture
setupSQLiteJobFixture = do
  store <- openBenchmarkSQLite
  enqueueJob store benchmarkJobId "payload" >>= requireJobEnqueued
  pure (SQLiteJobFixture store benchmarkLeaseWindows)


closeSQLiteCommitFixture :: SQLiteCommitFixture -> IO ()
closeSQLiteCommitFixture (SQLiteCommitFixture store _) = closeSQLiteStore store


closeSQLiteJobFixture :: SQLiteJobFixture -> IO ()
closeSQLiteJobFixture (SQLiteJobFixture store _) = closeSQLiteStore store


consumeMemory :: MemoryFixture -> IO Word64
consumeMemory (MemoryFixture store) = consumeEvents store


consumeSQLite :: SQLiteFixture -> IO Word64
consumeSQLite (SQLiteFixture store) = consumeEvents store


consumeEvents
  :: (EventStore backend, Show (BackendError backend))
  => backend
  -> IO Word64
consumeEvents store = do
  consumed <-
    runExceptT
      ( runConduit
          ( streamEventsAfter store (EventOffset 0)
              .| foldlC countEnvelope 0
          )
      )
  either (panic . show) pure consumed
  where
    countEnvelope count _ = count + 1


advanceMemoryProjection :: MemoryFixture -> IO Word64
advanceMemoryProjection (MemoryFixture store) = do
  advanced <-
    advanceProjection
      store
      (projectionUpdate projectionName (EventOffset 1) "view")
  pure case advanced of
    Right ProjectionAdvanced -> 1
    other -> panic (show other)


commitSQLiteFixture :: SQLiteCommitFixture -> IO Word64
commitSQLiteFixture (SQLiteCommitFixture store batch) = do
  committed <- commit store batch
  either (panic . show) (const (pure 1)) committed


claimMemoryJob :: MemoryJobFixture -> IO Word64
claimMemoryJob (MemoryJobFixture store windows) =
  claimJobs store windows


claimSQLiteJob :: SQLiteJobFixture -> IO Word64
claimSQLiteJob (SQLiteJobFixture store windows) =
  claimJobs store windows


claimJobs
  :: (JobStore backend, Show (BackendError backend))
  => backend
  -> [LeaseWindow]
  -> IO Word64
claimJobs store = foldM claim 0
  where
    claim _ window = do
      result <- claimJob store benchmarkJobId window
      case result of
        Right
          ( JobClaim
              (LeaseToken token)
              (AttemptCount attempts)
              payload
            ) ->
            ByteString.length payload `seq` token `seq` pure attempts
        other -> panic (show other)


benchmarkBatch :: Word64 -> IO CommitBatch
benchmarkBatch count =
  either
    (panic . show)
    pure
    (commitBatch benchmarkLimits (benchmarkAppend count :| []))


benchmarkAppend :: Word64 -> StreamAppend
benchmarkAppend count =
  appendEvents
    accountKey
    NoStream
    (Opened 0 :| replicateEvents (count - 1))


replicateEvents :: Word64 -> [AccountEvent]
replicateEvents count = Deposited 1 <$ [1 .. count]


requireRight :: Show error => Either error () -> IO ()
requireRight = either (panic . show) pure


requireJobEnqueued
  :: Show (BackendError backend)
  => Either (JobError backend) JobEnqueue
  -> IO ()
requireJobEnqueued result = case result of
  Right JobEnqueued -> pure ()
  other -> panic (show other)


openBenchmarkSQLite :: IO SQLiteStore
openBenchmarkSQLite =
  openSQLiteStore ":memory:" >>= either (panic . show) pure


benchmarkLimits :: CommitLimits
benchmarkLimits =
  mkCommitLimits
    (fromMaybe (panic "invalid payload limit") (mkPayloadLimit 4096))
    (fromMaybe (panic "invalid batch limit") (mkBatchLimit 20000))


projectionName :: ProjectionName
projectionName =
  fromMaybe (panic "invalid projection name") (mkProjectionName "benchmark")


benchmarkJobId :: JobId
benchmarkJobId =
  fromMaybe (panic "invalid benchmark job id") (mkJobId "benchmark-job")


benchmarkLeaseWindows :: [LeaseWindow]
benchmarkLeaseWindows = leaseWindow <$> [0 .. 999]
  where
    leaseWindow claimedAt =
      fromMaybe
        (panic "invalid benchmark lease window")
        ( mkLeaseWindow
            (LeaseInstant claimedAt)
            (LeaseInstant (claimedAt + 1))
        )


accountKey :: StreamKey Account
accountKey = streamKey (AccountId "benchmark")


accountMetadata :: AccountEvent -> EventMetadata
accountMetadata event =
  EventMetadata
    "benchmark-account"
    "benchmark"
    (Aggregate.eventType event)
    (Aggregate.eventVersion event)


stored :: Word64 -> AccountEvent -> StoredEvent
stored position event =
  StoredEvent
    (StreamPosition position)
    (accountMetadata event)
    (encodeEvent @Account event)
