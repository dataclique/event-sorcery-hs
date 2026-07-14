module Main (main) where

import Conduit (foldlC, runConduit, (.|))
import Criterion.Main
import Data.Aeson qualified as Aeson
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LazyByteString
import Data.IORef (atomicModifyIORef', newIORef)
import EventSorcery
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Backend.Memory
import EventSorcery.Backend.SQLite
import Protolude


newtype AccountId = AccountId Text


newtype Account = Account Word64
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data AccountCommand = Open | Notify


data AccountEvent
  = Opened Word64
  | Deposited Word64
  | NotificationQueued Text
  deriving stock (Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data AccountCommandError = AccountCommandError
  deriving stock (Eq, Show)


data AccountApplyError = AccountApplyError
  deriving stock (Eq, Show)


data BenchmarkProjectionError = BenchmarkProjectionError
  deriving stock (Eq, Show)


data BenchmarkReactorError = BenchmarkReactorError
  deriving stock (Eq, Show)


data BenchmarkJobFailure = BenchmarkJobFailure
  deriving stock (Eq, Show)


newtype BenchmarkJob = BenchmarkJob ByteString


newtype MemoryFixture = MemoryFixture MemoryStore


newtype SQLiteFixture = SQLiteFixture SQLiteStore


data SQLiteCommitFixture = SQLiteCommitFixture SQLiteStore CommitBatch


data MemoryJobFixture = MemoryJobFixture MemoryStore [LeaseWindow]


data SQLiteJobFixture = SQLiteJobFixture SQLiteStore [LeaseWindow]


data MemoryJobRuntimeFixture
  = MemoryJobRuntimeFixture (JobRuntime MemoryStore) LeaseWindow


data SQLiteJobRuntimeFixture
  = SQLiteJobRuntimeFixture
      SQLiteStore
      (JobRuntime SQLiteStore)
      LeaseWindow


data MemoryReactorFixture = MemoryReactorFixture MemoryStore [Word64]


data SQLiteReactorFixture = SQLiteReactorFixture SQLiteStore [Word64]


data MemoryOutboxFixture
  = MemoryOutboxFixture
      (OutboxRuntime MemoryStore)
      OutboxLeaseWindow


data SQLiteOutboxFixture
  = SQLiteOutboxFixture
      SQLiteStore
      (OutboxRuntime SQLiteStore)
      OutboxLeaseWindow


newtype MemoryDispatchFixture = MemoryDispatchFixture (Store MemoryStore Account)


data SQLiteDispatchFixture
  = SQLiteDispatchFixture SQLiteStore (Store SQLiteStore Account)


newtype MemoryLoadFixture = MemoryLoadFixture (Store MemoryStore Account)


data SQLiteLoadFixture
  = SQLiteLoadFixture SQLiteStore (Store SQLiteStore Account)


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


instance NFData MemoryJobRuntimeFixture where
  rnf (MemoryJobRuntimeFixture runtime window) = runtime `seq` window `seq` ()


instance NFData SQLiteJobRuntimeFixture where
  rnf (SQLiteJobRuntimeFixture store runtime window) =
    store `seq` runtime `seq` window `seq` ()


instance NFData MemoryReactorFixture where
  rnf (MemoryReactorFixture store offsets) = store `seq` rnf offsets


instance NFData SQLiteReactorFixture where
  rnf (SQLiteReactorFixture store offsets) = store `seq` rnf offsets


instance NFData MemoryOutboxFixture where
  rnf (MemoryOutboxFixture runtime window) = runtime `seq` window `seq` ()


instance NFData SQLiteOutboxFixture where
  rnf (SQLiteOutboxFixture store runtime window) =
    store `seq` runtime `seq` window `seq` ()


instance NFData MemoryDispatchFixture where
  rnf (MemoryDispatchFixture store) = store `seq` ()


instance NFData SQLiteDispatchFixture where
  rnf (SQLiteDispatchFixture backend store) = backend `seq` store `seq` ()


instance NFData MemoryLoadFixture where
  rnf (MemoryLoadFixture store) = store `seq` ()


instance NFData SQLiteLoadFixture where
  rnf (SQLiteLoadFixture backend store) = backend `seq` store `seq` ()


instance Job BenchmarkJob where
  jobType _ = "benchmark-job"
  encodeJob (BenchmarkJob payload) = payload
  decodeJob = Right . BenchmarkJob


instance DurableJob BenchmarkJob where
  type JobInput BenchmarkJob = ()
  type JobOutput BenchmarkJob = Word64
  type JobFailureCause BenchmarkJob = BenchmarkJobFailure


  submitJob _ _ (BenchmarkJob payload) =
    pure (Right (JobDone (fromIntegral (ByteString.length payload))))


  reconcileJob _ _ _ = pure (Right NotSubmitted)


instance Dispatches Account BenchmarkJob where
  injectDispatchIntent intent =
    NotificationQueued (jobIdText (dispatchJobId intent))


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[BenchmarkJob]


  aggregateType _ = "benchmark-account"
  encodeEntityId (AccountId identifier) = identifier
  eventType (Opened _) = "opened"
  eventType (Deposited _) = "deposited"
  eventType (NotificationQueued _) = "notification-queued"
  eventVersion _ = EventVersion 1
  schemaVersion _ = SchemaVersion 1
  encodeEvent = LazyByteString.toStrict . Aeson.encode
  decodeEvent =
    first (const (DecodeCause "invalid benchmark event"))
      . Aeson.eitherDecodeStrict'
  encodeSnapshot = LazyByteString.toStrict . Aeson.encode
  decodeSnapshot =
    first (const (DecodeCause "invalid benchmark snapshot"))
      . Aeson.eitherDecodeStrict'
  originate (Opened amount) = Right (Account amount)
  originate (Deposited _) = Left AccountApplyError
  originate (NotificationQueued _) = Left AccountApplyError
  evolve (Account balance) (Deposited amount) =
    Right (Account (balance + amount))
  evolve account (Opened _) = Right account
  evolve account (NotificationQueued _) = Right account
  initialize Open = Right (Events (Opened 0 :| []))
  initialize Notify = Left AccountCommandError
  transition _ Open = Left AccountCommandError
  transition _ Notify = Right (Dispatch (BenchmarkJob "payload"))


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
        , bench "memory/10000 catch-up" $
            perRunEnv setupMemoryFixture catchUpMemoryProjection
        , bench "sqlite/10000 catch-up" $
            perRunEnvWithCleanup
              setupSQLiteFixture
              closeSQLiteFixture
              catchUpSQLiteProjection
        , bench "memory/10000 rebuild" $
            perRunEnv setupMemoryRebuildFixture rebuildMemoryProjection
        , bench "sqlite/10000 rebuild" $
            perRunEnvWithCleanup
              setupSQLiteRebuildFixture
              closeSQLiteFixture
              rebuildSQLiteProjection
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
        , bench "memory/durable execution" $
            perRunEnv setupMemoryJobRuntimeFixture runMemoryJob
        , bench "sqlite/durable execution" $
            perRunEnvWithCleanup
              setupSQLiteJobRuntimeFixture
              closeSQLiteJobRuntimeFixture
              runSQLiteJob
        ]
    , bgroup
        "reactor"
        [ bench "memory/1000 outbox advances" $
            perRunEnv setupMemoryReactorFixture advanceMemoryReactor
        , bench "sqlite/1000 outbox advances" $
            perRunEnvWithCleanup
              setupSQLiteReactorFixture
              closeSQLiteReactorFixture
              advanceSQLiteReactor
        , bench "memory/10000 catch-up" $
            perRunEnv setupMemoryFixture catchUpMemoryReactor
        , bench "sqlite/10000 catch-up" $
            perRunEnvWithCleanup
              setupSQLiteFixture
              closeSQLiteFixture
              catchUpSQLiteReactor
        , bench "memory/outbox delivery" $
            perRunEnv setupMemoryOutboxFixture deliverMemoryOutbox
        , bench "sqlite/outbox delivery" $
            perRunEnvWithCleanup
              setupSQLiteOutboxFixture
              closeSQLiteOutboxFixture
              deliverSQLiteOutbox
        ]
    , bgroup
        "typed-store"
        [ bench "memory/initialize" $
            perRunEnv setupEmptyMemoryFixture executeMemoryOpen
        , bench "sqlite/initialize" $
            perRunEnvWithCleanup
              setupEmptySQLiteFixture
              closeSQLiteFixture
              executeSQLiteOpen
        , bench "memory/100 dispatches" $
            perRunEnv setupMemoryDispatchFixture executeMemoryNotify
        , bench "sqlite/100 dispatches" $
            perRunEnvWithCleanup
              setupSQLiteDispatchFixture
              closeSQLiteDispatchFixture
              executeSQLiteNotify
        ]
    , bgroup
        "snapshot"
        [ env setupMemoryLoadFixture $
            bench "memory/10000 full replay" . nfIO . loadMemoryEntity
        , env setupMemorySnapshotFixture $
            bench "memory/10000 snapshot load" . nfIO . loadMemoryEntity
        , envWithCleanup setupSQLiteLoadFixture closeSQLiteLoadFixture $
            bench "sqlite/10000 full replay" . nfIO . loadSQLiteEntity
        , envWithCleanup setupSQLiteSnapshotFixture closeSQLiteLoadFixture $
            bench "sqlite/10000 snapshot load" . nfIO . loadSQLiteEntity
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


setupEmptySQLiteFixture :: IO SQLiteFixture
setupEmptySQLiteFixture = SQLiteFixture <$> openBenchmarkSQLite


setupMemoryDispatchFixture :: IO MemoryDispatchFixture
setupMemoryDispatchFixture = do
  backend <- newMemoryStore
  store <- benchmarkStore backend
  _ <- executeTypedStoreCommand store Open
  pure (MemoryDispatchFixture store)


setupSQLiteDispatchFixture :: IO SQLiteDispatchFixture
setupSQLiteDispatchFixture = do
  backend <- openBenchmarkSQLite
  store <- benchmarkStore backend
  _ <- executeTypedStoreCommand store Open
  pure (SQLiteDispatchFixture backend store)


setupMemoryLoadFixture :: IO MemoryLoadFixture
setupMemoryLoadFixture = do
  MemoryFixture backend <- setupMemoryFixture
  MemoryLoadFixture <$> benchmarkStore backend


setupMemorySnapshotFixture :: IO MemoryLoadFixture
setupMemorySnapshotFixture = do
  fixture@(MemoryLoadFixture store) <- setupMemoryLoadFixture
  snapshotEntity store accountKey RetainedHistory >>= requireSnapshot
  pure fixture


setupSQLiteLoadFixture :: IO SQLiteLoadFixture
setupSQLiteLoadFixture = do
  SQLiteFixture backend <- setupSQLiteFixture
  store <- benchmarkStore backend
  pure (SQLiteLoadFixture backend store)


setupSQLiteSnapshotFixture :: IO SQLiteLoadFixture
setupSQLiteSnapshotFixture = do
  fixture@(SQLiteLoadFixture _ store) <- setupSQLiteLoadFixture
  snapshotEntity store accountKey RetainedHistory >>= requireSnapshot
  pure fixture


setupMemoryRebuildFixture :: IO MemoryFixture
setupMemoryRebuildFixture = do
  fixture <- setupMemoryFixture
  _ <- catchUpMemoryProjection fixture
  pure fixture


setupSQLiteRebuildFixture :: IO SQLiteFixture
setupSQLiteRebuildFixture = do
  fixture <- setupSQLiteFixture
  _ <- catchUpSQLiteProjection fixture
  pure fixture


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


setupMemoryJobRuntimeFixture :: IO MemoryJobRuntimeFixture
setupMemoryJobRuntimeFixture = do
  store <- newMemoryStore
  let runtime =
        mkJobRuntime store benchmarkAttemptLimit benchmarkJobRetrySchedule
  enqueueDurableJob runtime benchmarkJobId (BenchmarkJob "payload")
    >>= requireJobEnqueued
  pure (MemoryJobRuntimeFixture runtime benchmarkJobLeaseWindow)


setupSQLiteJobRuntimeFixture :: IO SQLiteJobRuntimeFixture
setupSQLiteJobRuntimeFixture = do
  store <- openBenchmarkSQLite
  let runtime =
        mkJobRuntime store benchmarkAttemptLimit benchmarkJobRetrySchedule
  enqueueDurableJob runtime benchmarkJobId (BenchmarkJob "payload")
    >>= requireJobEnqueued
  pure (SQLiteJobRuntimeFixture store runtime benchmarkJobLeaseWindow)


setupMemoryReactorFixture :: IO MemoryReactorFixture
setupMemoryReactorFixture =
  MemoryReactorFixture <$> newMemoryStore <*> pure benchmarkReactorOffsets


setupSQLiteReactorFixture :: IO SQLiteReactorFixture
setupSQLiteReactorFixture = do
  store <- openBenchmarkSQLite
  pure (SQLiteReactorFixture store benchmarkReactorOffsets)


setupMemoryOutboxFixture :: IO MemoryOutboxFixture
setupMemoryOutboxFixture = do
  store <- newMemoryStore
  advanceReactor store benchmarkFirstReactorUpdate >>= requireReactorCommit
  pure
    ( MemoryOutboxFixture
        (mkOutboxRuntime store benchmarkOutboxLimit benchmarkOutboxRetrySchedule)
        benchmarkOutboxLeaseWindow
    )


setupSQLiteOutboxFixture :: IO SQLiteOutboxFixture
setupSQLiteOutboxFixture = do
  store <- openBenchmarkSQLite
  advanceReactor store benchmarkFirstReactorUpdate >>= requireReactorCommit
  pure
    ( SQLiteOutboxFixture
        store
        (mkOutboxRuntime store benchmarkOutboxLimit benchmarkOutboxRetrySchedule)
        benchmarkOutboxLeaseWindow
    )


closeSQLiteCommitFixture :: SQLiteCommitFixture -> IO ()
closeSQLiteCommitFixture (SQLiteCommitFixture store _) = closeSQLiteStore store


closeSQLiteJobFixture :: SQLiteJobFixture -> IO ()
closeSQLiteJobFixture (SQLiteJobFixture store _) = closeSQLiteStore store


closeSQLiteJobRuntimeFixture :: SQLiteJobRuntimeFixture -> IO ()
closeSQLiteJobRuntimeFixture (SQLiteJobRuntimeFixture store _ _) =
  closeSQLiteStore store


closeSQLiteReactorFixture :: SQLiteReactorFixture -> IO ()
closeSQLiteReactorFixture (SQLiteReactorFixture store _) =
  closeSQLiteStore store


closeSQLiteOutboxFixture :: SQLiteOutboxFixture -> IO ()
closeSQLiteOutboxFixture (SQLiteOutboxFixture store _ _) =
  closeSQLiteStore store


closeSQLiteDispatchFixture :: SQLiteDispatchFixture -> IO ()
closeSQLiteDispatchFixture (SQLiteDispatchFixture backend _) =
  closeSQLiteStore backend


closeSQLiteLoadFixture :: SQLiteLoadFixture -> IO ()
closeSQLiteLoadFixture (SQLiteLoadFixture backend _) = closeSQLiteStore backend


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


catchUpMemoryProjection :: MemoryFixture -> IO Word64
catchUpMemoryProjection (MemoryFixture store) =
  requireProjectionResult (catchUpProjection store benchmarkProjection)


catchUpSQLiteProjection :: SQLiteFixture -> IO Word64
catchUpSQLiteProjection (SQLiteFixture store) =
  requireProjectionResult (catchUpProjection store benchmarkProjection)


rebuildMemoryProjection :: MemoryFixture -> IO Word64
rebuildMemoryProjection (MemoryFixture store) =
  requireProjectionResult (rebuildProjection store benchmarkProjection)


rebuildSQLiteProjection :: SQLiteFixture -> IO Word64
rebuildSQLiteProjection (SQLiteFixture store) =
  requireProjectionResult (rebuildProjection store benchmarkProjection)


requireProjectionResult
  :: (Show (BackendError backend), Show projectionError)
  => IO (Either (ProjectionRunError backend projectionError) Word64)
  -> IO Word64
requireProjectionResult action = action >>= either (panic . show) pure


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


runMemoryJob :: MemoryJobRuntimeFixture -> IO Word64
runMemoryJob (MemoryJobRuntimeFixture runtime window) =
  runBenchmarkJob runtime window


runSQLiteJob :: SQLiteJobRuntimeFixture -> IO Word64
runSQLiteJob (SQLiteJobRuntimeFixture _ runtime window) =
  runBenchmarkJob runtime window


runBenchmarkJob
  :: (JobStore backend, Show (BackendError backend))
  => JobRuntime backend
  -> LeaseWindow
  -> IO Word64
runBenchmarkJob runtime window = do
  result <-
    runJobOnce
      (Proxy @BenchmarkJob)
      runtime
      ()
      benchmarkJobId
      window
  pure case result of
    Right (JobSucceeded payloadSize) -> payloadSize
    other -> panic (show other)


advanceMemoryReactor :: MemoryReactorFixture -> IO Word64
advanceMemoryReactor (MemoryReactorFixture store offsets) =
  advanceReactors store offsets


advanceSQLiteReactor :: SQLiteReactorFixture -> IO Word64
advanceSQLiteReactor (SQLiteReactorFixture store offsets) =
  advanceReactors store offsets


advanceReactors
  :: (ReactorStore backend, Show (BackendError backend))
  => backend
  -> [Word64]
  -> IO Word64
advanceReactors store = foldM advance 0
  where
    advance _ offset = do
      result <-
        advanceReactor
          store
          ( reactorUpdate
              benchmarkReactorName
              (EventOffset offset)
              (Just (benchmarkOutboxEntry offset))
          )
      case result of
        Right ReactorCommitted -> pure offset
        other -> panic (show other)


catchUpMemoryReactor :: MemoryFixture -> IO Word64
catchUpMemoryReactor (MemoryFixture store) =
  requireReactorResult (catchUpReactor store benchmarkReactor)


catchUpSQLiteReactor :: SQLiteFixture -> IO Word64
catchUpSQLiteReactor (SQLiteFixture store) =
  requireReactorResult (catchUpReactor store benchmarkReactor)


requireReactorResult
  :: (Show (BackendError backend), Show reactorError)
  => IO (Either (ReactorRunError backend reactorError) EventOffset)
  -> IO Word64
requireReactorResult action = do
  result <- action
  pure case result of
    Right (EventOffset offset) -> offset
    Left failure -> panic (show failure)


deliverMemoryOutbox :: MemoryOutboxFixture -> IO Word64
deliverMemoryOutbox (MemoryOutboxFixture runtime window) =
  deliverBenchmarkOutbox runtime window


deliverSQLiteOutbox :: SQLiteOutboxFixture -> IO Word64
deliverSQLiteOutbox (SQLiteOutboxFixture _ runtime window) =
  deliverBenchmarkOutbox runtime window


deliverBenchmarkOutbox
  :: (ReactorStore backend, Show (BackendError backend))
  => OutboxRuntime backend
  -> OutboxLeaseWindow
  -> IO Word64
deliverBenchmarkOutbox runtime window = do
  result <-
    runOutboxOnce
      runtime
      benchmarkDeliveryId
      window
      benchmarkDelivery
  pure case result of
    Right DeliverySucceeded -> 1
    other -> panic (show other)


benchmarkDelivery
  :: DeliveryId
  -> OutboxPayload
  -> IO (Either (OutboxDeliveryFailure BenchmarkJobFailure) ())
benchmarkDelivery _ payload = evaluate (forceOutboxPayload payload) $> Right ()


forceOutboxPayload :: OutboxPayload -> Int
forceOutboxPayload payload = case payload of
  CommandDelivery bytes -> ByteString.length bytes
  JobDispatch bytes -> ByteString.length bytes


executeMemoryOpen :: MemoryFixture -> IO Word64
executeMemoryOpen (MemoryFixture backend) = executeStoreCommand backend Open


executeSQLiteOpen :: SQLiteFixture -> IO Word64
executeSQLiteOpen (SQLiteFixture backend) = executeStoreCommand backend Open


executeMemoryNotify :: MemoryDispatchFixture -> IO Word64
executeMemoryNotify (MemoryDispatchFixture store) = executeDispatches store


executeSQLiteNotify :: SQLiteDispatchFixture -> IO Word64
executeSQLiteNotify (SQLiteDispatchFixture _ store) = executeDispatches store


executeStoreCommand
  :: (EventStore backend, Show (BackendError backend))
  => backend
  -> AccountCommand
  -> IO Word64
executeStoreCommand backend command = do
  store <- benchmarkStore backend
  executeTypedStoreCommand store command


executeDispatches
  :: (EventStore backend, Show (BackendError backend))
  => Store backend Account
  -> IO Word64
executeDispatches store = foldM execute 0 ([1 .. 100] :: [Word16])
  where
    execute _ _ = executeTypedStoreCommand store Notify


executeTypedStoreCommand
  :: (EventStore backend, Show (BackendError backend))
  => Store backend Account
  -> AccountCommand
  -> IO Word64
executeTypedStoreCommand store command = do
  result <-
    executeCommand
      store
      accountKey
      command
  pure case result of
    Right (Account balance) -> balance
    other -> panic (show other)


loadMemoryEntity :: MemoryLoadFixture -> IO Word64
loadMemoryEntity (MemoryLoadFixture store) = loadBenchmarkEntity store


loadSQLiteEntity :: SQLiteLoadFixture -> IO Word64
loadSQLiteEntity (SQLiteLoadFixture _ store) = loadBenchmarkEntity store


loadBenchmarkEntity
  :: (EventStore backend, Show (BackendError backend))
  => Store backend Account
  -> IO Word64
loadBenchmarkEntity store = do
  loaded <- loadEntity store accountKey
  pure case loaded of
    Right (Just (Account balance)) -> balance
    other -> panic (show other)


benchmarkStore :: backend -> IO (Store backend Account)
benchmarkStore backend = do
  nextIdentifier <- newIORef (1 :: Word64)
  pure
    ( mkStore backend benchmarkLimits do
        identifier <- atomicModifyIORef' nextIdentifier (\value -> (value + 1, value))
        pure
          ( fromMaybe
              (panic "invalid benchmark job id")
              (mkJobId ("benchmark-job-" <> show identifier))
          )
    )


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


requireSnapshot
  :: Show (BackendError backend)
  => Either (StoreError backend Account) (Maybe Account)
  -> IO ()
requireSnapshot result = case result of
  Right (Just _) -> pure ()
  other -> panic (show other)


requireJobEnqueued
  :: Show (BackendError backend)
  => Either (JobError backend) JobEnqueue
  -> IO ()
requireJobEnqueued result = case result of
  Right JobEnqueued -> pure ()
  other -> panic (show other)


requireReactorCommit
  :: Show (BackendError backend)
  => Either (ReactorError backend) ReactorCommit
  -> IO ()
requireReactorCommit result = case result of
  Right ReactorCommitted -> pure ()
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


benchmarkProjection :: Projection Account Word64 BenchmarkProjectionError
benchmarkProjection =
  Projection
    { name = projectionName
    , version = SchemaVersion 1
    , initial = 0
    , apply = applyBenchmarkProjection
    , encode = LazyByteString.toStrict . Aeson.encode
    , decode =
        first (const (DecodeCause "invalid benchmark projection"))
          . Aeson.eitherDecodeStrict'
    }


applyBenchmarkProjection
  :: Word64 -> AccountEvent -> Either BenchmarkProjectionError Word64
applyBenchmarkProjection balance event = case event of
  Opened amount -> Right (balance + amount)
  Deposited amount -> Right (balance + amount)
  NotificationQueued _ -> Right balance


benchmarkJobId :: JobId
benchmarkJobId =
  fromMaybe (panic "invalid benchmark job id") (mkJobId "benchmark-job")


benchmarkAttemptLimit :: AttemptLimit
benchmarkAttemptLimit =
  fromMaybe (panic "invalid benchmark attempt limit") (mkAttemptLimit 3)


benchmarkJobRetrySchedule :: AttemptCount -> LeaseInstant
benchmarkJobRetrySchedule (AttemptCount attempt) = LeaseInstant (20 + attempt)


benchmarkJobLeaseWindow :: LeaseWindow
benchmarkJobLeaseWindow =
  fromMaybe
    (panic "invalid benchmark job lease")
    (mkLeaseWindow (LeaseInstant 0) (LeaseInstant 10))


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


benchmarkReactorName :: ReactorName
benchmarkReactorName =
  fromMaybe
    (panic "invalid benchmark reactor name")
    (mkReactorName "benchmark-reactor")


benchmarkReactor :: Reactor Account BenchmarkReactorError
benchmarkReactor = Reactor benchmarkReactorName (\_ _ -> Right Nothing)


benchmarkReactorOffsets :: [Word64]
benchmarkReactorOffsets = [1 .. 1000]


benchmarkDeliveryId :: DeliveryId
benchmarkDeliveryId =
  fromMaybe
    (panic "invalid benchmark delivery id")
    (mkDeliveryId "benchmark-delivery")


benchmarkFirstReactorUpdate :: ReactorUpdate
benchmarkFirstReactorUpdate =
  reactorUpdate
    benchmarkReactorName
    (EventOffset 1)
    (Just (OutboxEntry benchmarkDeliveryId (CommandDelivery "payload")))


benchmarkOutboxLimit :: OutboxAttemptLimit
benchmarkOutboxLimit =
  fromMaybe (panic "invalid benchmark outbox limit") (mkOutboxAttemptLimit 3)


benchmarkOutboxRetrySchedule :: OutboxAttempt -> OutboxInstant
benchmarkOutboxRetrySchedule (OutboxAttempt attempt) =
  OutboxInstant (20 + attempt)


benchmarkOutboxLeaseWindow :: OutboxLeaseWindow
benchmarkOutboxLeaseWindow =
  fromMaybe
    (panic "invalid benchmark outbox lease")
    (mkOutboxLeaseWindow (OutboxInstant 0) (OutboxInstant 10))


benchmarkOutboxEntry :: Word64 -> OutboxEntry
benchmarkOutboxEntry offset =
  OutboxEntry
    ( fromMaybe
        (panic "invalid benchmark delivery id")
        (mkDeliveryId ("benchmark-delivery-" <> show offset))
    )
    (CommandDelivery "payload")


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
