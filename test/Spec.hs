module Main (main) where

import Conduit (runConduit, sinkList, (.|))
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as LazyByteString
import Data.IORef
import EventSorcery
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Backend.Memory
import EventSorcery.Backend.SQLite
import Protolude
import Test.Hspec


newtype AccountId = AccountId Text
  deriving stock (Eq, Show)


newtype Account = Account Word64
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


newtype AccountV2Id = AccountV2Id Text


newtype AccountV2 = AccountV2 Word64
  deriving stock (Eq, Show)


newtype AccountV2Event = AccountV2Event AccountEvent


data AccountCommand
  = Open Word64
  | Deposit Word64
  | Notify
  | InvalidDeposit
  deriving stock (Eq, Show)


data AccountEvent
  = Opened Word64
  | Deposited Word64
  | NotificationQueued Text
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data AccountCommandError = AlreadyOpen
  deriving stock (Eq, Show)


data AccountApplyError = DepositBeforeOpen
  deriving stock (Eq, Show)


newtype BalanceView = BalanceView Word64
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data BalanceProjectionError = BalanceProjectionError
  deriving stock (Eq, Show)


data BalanceReactorError = BalanceReactorError
  deriving stock (Eq, Show)


newtype EmailJob = EmailJob Text


instance Job EmailJob where
  jobType _ = "email"
  encodeJob (EmailJob recipient) = encodeUtf8 recipient
  decodeJob = Right . EmailJob . decodeUtf8


data ProbeSubmission
  = SubmitSucceeds
  | SubmitTransientlyFails
  | SubmitTerminallyFails
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data ProbeReconciliation
  = ReconcileAsNotSubmitted
  | ReconcileAsIndeterminate
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data ProbeJob = ProbeJob ProbeSubmission ProbeReconciliation
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data ProbeInvocation
  = SubmitInvoked
  | ReconcileInvoked
  deriving stock (Eq, Show)


newtype ProbeInput = ProbeInput (IORef [ProbeInvocation])


data ProbeFailure = ProbeFailure
  deriving stock (Eq, Show)


instance Job ProbeJob where
  jobType _ = "probe"
  encodeJob = LazyByteString.toStrict . Aeson.encode
  decodeJob =
    first (const (DecodeCause "invalid probe job")) . Aeson.eitherDecodeStrict'


instance DurableJob ProbeJob where
  type JobInput ProbeJob = ProbeInput
  type JobOutput ProbeJob = Text
  type JobFailureCause ProbeJob = ProbeFailure


  submitJob _ input (ProbeJob submission _) = do
    recordProbeInvocation input SubmitInvoked
    pure case submission of
      SubmitSucceeds -> Right (JobDone "submitted")
      SubmitTransientlyFails -> Left (Transient ProbeFailure)
      SubmitTerminallyFails -> Left (Terminal ProbeFailure)


  reconcileJob _ input (ProbeJob _ reconciliation) = do
    recordProbeInvocation input ReconcileInvoked
    pure case reconciliation of
      ReconcileAsNotSubmitted -> Right NotSubmitted
      ReconcileAsIndeterminate -> Right (Indeterminate (LeaseInstant 40))


instance Dispatches Account EmailJob where
  injectDispatchIntent intent =
    NotificationQueued (jobIdText (dispatchJobId intent))


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[EmailJob]


  aggregateType _ = "account"
  encodeEntityId (AccountId identifier) = identifier
  eventType (Opened _) = "opened"
  eventType (Deposited _) = "deposited"
  eventType (NotificationQueued _) = "notification-queued"
  eventVersion _ = EventVersion 1
  schemaVersion _ = SchemaVersion 1
  encodeEvent = LazyByteString.toStrict . Aeson.encode
  decodeEvent =
    first (const (DecodeCause "invalid account event")) . Aeson.eitherDecodeStrict'
  encodeSnapshot = LazyByteString.toStrict . Aeson.encode
  decodeSnapshot =
    first (const (DecodeCause "invalid account snapshot"))
      . Aeson.eitherDecodeStrict'
  originate (Opened amount) = Right (Account amount)
  originate (Deposited _) = Left DepositBeforeOpen
  originate (NotificationQueued _) = Left DepositBeforeOpen
  evolve (Account balance) (Deposited amount) = Right (Account (balance + amount))
  evolve account (Opened _) = Right account
  evolve account (NotificationQueued _) = Right account
  initialize (Open amount) = Right (Events (Opened amount :| []))
  initialize (Deposit _) = Left AlreadyOpen
  initialize Notify = Left AlreadyOpen
  initialize InvalidDeposit = Right (Events (Deposited 1 :| []))
  transition _ (Open _) = Left AlreadyOpen
  transition _ (Deposit amount) = Right (Events (Deposited amount :| []))
  transition _ Notify = Right (Dispatch (EmailJob "owner@example.com"))
  transition _ InvalidDeposit = Right (Events (Deposited 1 :| []))


instance EventSourced AccountV2 where
  type EntityId AccountV2 = AccountV2Id
  type Command AccountV2 = AccountCommand
  type Event AccountV2 = AccountV2Event
  type CommandError AccountV2 = AccountCommandError
  type ApplyError AccountV2 = AccountApplyError
  type Jobs AccountV2 = '[]


  aggregateType _ = "account"
  encodeEntityId (AccountV2Id identifier) = identifier
  eventType (AccountV2Event event) = Aggregate.eventType @Account event
  eventVersion (AccountV2Event event) = Aggregate.eventVersion @Account event
  schemaVersion _ = SchemaVersion 2
  encodeEvent (AccountV2Event event) = encodeEvent @Account event
  decodeEvent = fmap AccountV2Event . decodeEvent @Account
  encodeSnapshot (AccountV2 balance) =
    LazyByteString.toStrict (Aeson.encode balance)
  decodeSnapshot bytes =
    first
      (const (DecodeCause "invalid account v2 snapshot"))
      (AccountV2 <$> Aeson.eitherDecodeStrict' bytes)
  originate (AccountV2Event event) = toV2 <$> originate @Account event
  evolve (AccountV2 balance) (AccountV2Event event) =
    toV2 <$> evolve @Account (Account balance) event
  initialize _ = Left AlreadyOpen
  transition _ _ = Left AlreadyOpen


main :: IO ()
main = hspec do
  describe "replay" do
    it "replays an ordered stream into aggregate state" do
      replay accountKey [stored 1 (Opened 10), stored 2 (Deposited 5)]
        `shouldBe` Right (Just (Account 15))

    it "rejects a sequence gap" do
      replay accountKey [stored 1 (Opened 10), stored 3 (Deposited 5)]
        `shouldBe` Left
          ( EventSequenceMismatch
              (ExpectedSequence (StreamPosition 2))
              (ActualSequence (StreamPosition 3))
          )

    it "rejects aggregate metadata spoofing" do
      let spoofed = (stored 1 (Opened 10)) {metadata = accountMetadata {aggregateType = "other"}}
      replay accountKey [spoofed]
        `shouldBe` Left
          ( EventMetadataMismatch
              (StreamPosition 1)
              (AggregateTypeMismatch "account" "other")
          )

    it "redacts malformed payloads from decode failures" do
      let malformed = StoredEvent (StreamPosition 1) accountMetadata "not-json"
      replay accountKey [malformed]
        `shouldBe` Left
          (EventDecodeFailed (StreamPosition 1) (DecodeCause "invalid account event"))

  describe "commit batch validation" do
    it "rejects event payloads over the configured limit" do
      let append = appendEvents accountKey NoStream (Opened 10 :| [])
      isOversizedPayload
        (commitBatch (mkCommitLimits (payloadLimit 1) (batchLimit 16)) (append :| []))
        `shouldBe` True

    it "rejects batches over the configured event limit" do
      let appends =
            appendEvents accountKey NoStream (Opened 10 :| [])
              :| [appendEvents secondAccountKey NoStream (Opened 20 :| [])]
      isOversizedBatch
        (commitBatch (mkCommitLimits (payloadLimit 1024) (batchLimit 1)) appends)
        `shouldBe` True

    it "rejects more than one append for the same stream" do
      let append = appendEvents accountKey NoStream (Opened 10 :| [])
      isDuplicateStream (commitBatch testLimits (append :| [append]))
        `shouldBe` True

  describe "lease window validation" do
    it "rejects a lease that does not advance time" do
      mkLeaseWindow (LeaseInstant 20) (LeaseInstant 20)
        `shouldBe` Nothing

  eventStoreContract "in-memory event store" withMemoryStore
  eventStoreContract "SQLite event store" withSQLiteStore
  projectionStoreContract "in-memory projection store" withMemoryStore
  projectionStoreContract "SQLite projection store" withSQLiteStore
  deliveryStoreContract "in-memory delivery store" withMemoryStore
  deliveryStoreContract "SQLite delivery store" withSQLiteStore
  commandDeliveryContract "in-memory command delivery" withMemoryStore
  commandDeliveryContract "SQLite command delivery" withSQLiteStore
  jobStoreContract "in-memory job store" withMemoryStore
  jobStoreContract "SQLite job store" withSQLiteStore
  jobRuntimeContract "in-memory job runtime" withMemoryStore
  jobRuntimeContract "SQLite job runtime" withSQLiteStore
  reactorStoreContract "in-memory reactor store" withMemoryStore
  reactorStoreContract "SQLite reactor store" withSQLiteStore
  reactorRunnerContract "in-memory reactor runner" withMemoryStore
  reactorRunnerContract "SQLite reactor runner" withSQLiteStore
  outboxRuntimeContract "in-memory outbox runtime" withMemoryStore
  outboxRuntimeContract "SQLite outbox runtime" withSQLiteStore
  schemaStoreContract "in-memory schema store" withMemoryStore
  schemaStoreContract "SQLite schema store" withSQLiteStore
  storeContract "in-memory typed store" withMemoryStore
  storeContract "SQLite typed store" withSQLiteStore
  snapshotContract "in-memory snapshots" withMemoryStore
  snapshotContract "SQLite snapshots" withSQLiteStore


eventStoreContract
  :: forall backend
   . ( Eq (BackendError backend)
     , EventStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
eventStoreContract label withStore = describe label do
  it "streams committed events from a durable global cursor" $ withStore \store -> do
    batch <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
    commit store batch `shouldReturn` Right ()
    streamed <-
      runExceptT
        (runConduit (streamEventsAfter store (EventOffset 0) .| sinkList))
    streamed
      `shouldBe` Right
        [StoredEnvelope (EventOffset 1) accountIdentity (stored 1 (Opened 10))]

  it "appends and loads an ordered stream" $ withStore \store -> do
    batch <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
    commit store batch `shouldReturn` Right ()
    loaded <- loadStream store accountIdentity
    loaded `shouldSatisfy` isRight
    (replay accountKey <$> loaded) `shouldBe` Right (Right (Just (Account 10)))

  it "reports an explicit expected-version conflict without mutation" $
    withStore \store -> do
      original <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
      commit store original `shouldReturn` Right ()
      conflicting <-
        validBatch (appendEvents accountKey NoStream (Deposited 5 :| []))
      commit store conflicting
        `shouldReturn` Left
          (ConcurrencyConflict accountIdentity NoStream (At (StreamVersion 1)))
      loaded <- loadStream store accountIdentity
      (replay accountKey <$> loaded)
        `shouldBe` Right (Right (Just (Account 10)))

  it "leaves every stream unchanged when one append in a batch conflicts" $
    withStore \store -> do
      original <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
      commit store original `shouldReturn` Right ()
      batch <-
        validBatchFrom
          ( appendEvents accountKey NoStream (Deposited 5 :| [])
              :| [appendEvents secondAccountKey NoStream (Opened 20 :| [])]
          )
      commit store batch
        `shouldReturn` Left
          (ConcurrencyConflict accountIdentity NoStream (At (StreamVersion 1)))
      secondLoaded <- loadStream store secondAccountIdentity
      secondLoaded `shouldBe` Right []

  it "allows exactly one of several concurrent writers to create a stream" $
    withStore \store -> do
      start <- newEmptyMVar
      finished <- newEmptyMVar
      let writerAmounts = [10, 20, 30, 40]
          runWriter amount = do
            takeMVar start
            batch <- validBatch (appendEvents accountKey NoStream (Opened amount :| []))
            commit store batch >>= putMVar finished
      traverse_ (void . forkIO . runWriter) writerAmounts
      traverse_ (const (putMVar start ())) writerAmounts
      outcomes <- traverse (const (takeMVar finished)) writerAmounts
      length (rights outcomes) `shouldBe` 1
      lefts outcomes
        `shouldBe` replicate
          (length writerAmounts - 1)
          (ConcurrencyConflict accountIdentity NoStream (At (StreamVersion 1)))
      loaded <- loadStream store accountIdentity
      fmap length loaded `shouldBe` Right 1


projectionStoreContract
  :: forall backend
   . ( Eq (BackendError backend)
     , ProjectionStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
projectionStoreContract label withStore = describe label do
  it "stores a view and its checkpoint in one advance" $ withStore \store -> do
    advanceProjection store firstProjectionUpdate
      `shouldReturn` Right ProjectionAdvanced
    loadProjection store balancesProjectionName
      `shouldReturn` Right
        (Just (ProjectionState (EventOffset 1) "view-at-one"))

  it "absorbs a repeated current envelope without replacing the view" $
    withStore \store -> do
      advanceProjection store firstProjectionUpdate
        `shouldReturn` Right ProjectionAdvanced
      advanceProjection store repeatedProjectionUpdate
        `shouldReturn` Right ProjectionAlreadyApplied
      loadProjection store balancesProjectionName
        `shouldReturn` Right
          (Just (ProjectionState (EventOffset 1) "view-at-one"))

  it "rejects a skipped envelope without storing its view" $ withStore \store -> do
    advanceProjection store skippedProjectionUpdate
      `shouldReturn` Left
        ( ProjectionSequenceMismatch
            balancesProjectionName
            (EventOffset 1)
            (EventOffset 2)
        )
    loadProjection store balancesProjectionName `shouldReturn` Right Nothing

  it "resets rebuildable projection state" $ withStore \store -> do
    advanceProjection store firstProjectionUpdate
      `shouldReturn` Right ProjectionAdvanced
    resetProjection store balancesProjectionName `shouldReturn` Right ()
    loadProjection store balancesProjectionName `shouldReturn` Right Nothing

  it "catches up incrementally and rebuilds from the global event log" $
    withStore \store -> do
      initial <-
        validBatchFrom
          ( appendEvents accountKey NoStream (Opened 10 :| [])
              :| [appendEvents secondAccountKey NoStream (Opened 20 :| [])]
          )
      commit store initial `shouldReturn` Right ()
      catchUpProjection store balanceProjection
        `shouldReturn` Right (BalanceView 30)
      loaded <- loadProjection store balancesProjectionName
      fmap (fmap projectionCheckpoint) loaded
        `shouldBe` Right (Just (EventOffset 2))
      deposit <-
        validBatch
          ( appendEvents
              accountKey
              (At (StreamVersion 1))
              (Deposited 5 :| [])
          )
      commit store deposit `shouldReturn` Right ()
      catchUpProjection store balanceProjection
        `shouldReturn` Right (BalanceView 35)
      rebuildProjection store balanceProjection
        `shouldReturn` Right (BalanceView 35)
      rebuildProjections store [balanceProjection]
        `shouldReturn` Right [BalanceView 35]

  it "rejects a malformed persisted view with a typed decode failure" $
    withStore \store -> do
      advanceProjection
        store
        (projectionUpdate balancesProjectionName (EventOffset 1) "not-json")
        `shouldReturn` Right ProjectionAdvanced
      catchUpProjection store balanceProjection
        `shouldReturn` Left
          ( ProjectionViewDecodeFailed
              balancesProjectionName
              (DecodeCause "invalid balance projection")
          )

  it "skips framework job events while advancing the global checkpoint" $
    withStore \store -> do
      let typedStore = mkStore store testLimits (pure jobId)
      executeCommand typedStore accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand typedStore accountKey Notify
        `shouldReturn` Right (Account 10)
      catchUpProjection store balanceProjection
        `shouldReturn` Right (BalanceView 10)
      loaded <- loadProjection store balancesProjectionName
      fmap (fmap projectionCheckpoint) loaded
        `shouldBe` Right (Just (EventOffset 3))


deliveryStoreContract
  :: forall backend
   . ( DeliveryStore backend
     , Eq (BackendError backend)
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
deliveryStoreContract label withStore = describe label do
  it "records a receipt with target events and absorbs a repeated delivery" $
    withStore \store -> do
      firstBatch <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
      commitDelivery store deliveryId firstBatch
        `shouldReturn` Right DeliveryApplied
      repeatedBatch <-
        validBatch
          ( appendEvents
              accountKey
              (At (StreamVersion 1))
              (Deposited 5 :| [])
          )
      commitDelivery store deliveryId repeatedBatch
        `shouldReturn` Right DeliveryAlreadyApplied
      loaded <- loadStream store accountIdentity
      (replay accountKey <$> loaded)
        `shouldBe` Right (Right (Just (Account 10)))

  it "does not record a receipt when the target commit conflicts" $
    withStore \store -> do
      original <- validBatch (appendEvents accountKey NoStream (Opened 10 :| []))
      commit store original `shouldReturn` Right ()
      conflicting <-
        validBatch (appendEvents accountKey NoStream (Deposited 5 :| []))
      commitDelivery store deliveryId conflicting
        `shouldReturn` Left
          (ConcurrencyConflict accountIdentity NoStream (At (StreamVersion 1)))
      correctedBatch <-
        validBatch
          ( appendEvents
              accountKey
              (At (StreamVersion 1))
              (Deposited 5 :| [])
          )
      commitDelivery store deliveryId correctedBatch
        `shouldReturn` Right DeliveryApplied
      loaded <- loadStream store accountIdentity
      (replay accountKey <$> loaded)
        `shouldBe` Right (Right (Just (Account 15)))


commandDeliveryContract
  :: forall backend
   . ( DeliveryStore backend
     , Eq (BackendError backend)
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
commandDeliveryContract label withBackend = describe label do
  it "does not decide or append an acknowledged command twice" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      deliverCommand store deliveryId accountKey (Deposit 5)
        `shouldReturn` Right DeliveryApplied
      deliverCommand store deliveryId accountKey (Deposit 100)
        `shouldReturn` Right DeliveryAlreadyApplied
      loadEntity store accountKey `shouldReturn` Right (Just (Account 15))


jobStoreContract
  :: forall backend
   . ( JobStore backend
     , Eq (BackendError backend)
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
jobStoreContract label withStore = describe label do
  it "enqueues the same payload idempotently and rejects a changed payload" $
    withStore \store -> do
      enqueueJob store jobId "payload" `shouldReturn` Right JobEnqueued
      enqueueJob store jobId "payload"
        `shouldReturn` Right JobAlreadyEnqueued
      enqueueJob store jobId "changed"
        `shouldReturn` Left (JobPayloadMismatch jobId)

  it "fences a worker whose lease expired and was reclaimed" $
    withStore \store -> do
      enqueueJob store jobId "payload" `shouldReturn` Right JobEnqueued
      claimJob store jobId firstLease
        `shouldReturn` Right
          (JobClaim (LeaseToken 1) (AttemptCount 0) "payload")
      claimJob store jobId overlappingLease
        `shouldReturn` Left
          (JobLeaseUnavailable jobId (LeaseInstant 20))
      claimJob store jobId replacementLease
        `shouldReturn` Right
          (JobClaim (LeaseToken 2) (AttemptCount 0) "payload")
      acknowledgeJob store jobId (LeaseToken 1)
        `shouldReturn` Left
          (JobLeaseLost jobId (LeaseToken 1) (LeaseToken 2))
      acknowledgeJob store jobId (LeaseToken 2) `shouldReturn` Right ()
      acknowledgeJob store jobId (LeaseToken 2) `shouldReturn` Right ()
      claimJob store jobId completedLease
        `shouldReturn` Left (JobAlreadyCompleted jobId)

  it "defers without an attempt and counts only transient failures" $
    withStore \store -> do
      enqueueJob store jobId "payload" `shouldReturn` Right JobEnqueued
      claimJob store jobId firstLease
        `shouldReturn` Right
          (JobClaim (LeaseToken 1) (AttemptCount 0) "payload")
      deferJob store jobId (LeaseToken 1) (LeaseInstant 30)
        `shouldReturn` Right ()
      claimJob store jobId replacementLease
        `shouldReturn` Left (JobNotRunnable jobId (LeaseInstant 30))
      claimJob store jobId completedLease
        `shouldReturn` Right
          (JobClaim (LeaseToken 2) (AttemptCount 0) "payload")
      retryJob store jobId (LeaseToken 2) (LeaseInstant 50)
        `shouldReturn` Right (AttemptCount 1)
      claimJob store jobId (leaseWindow 40 60)
        `shouldReturn` Left (JobNotRunnable jobId (LeaseInstant 50))
      claimJob store jobId (leaseWindow 50 60)
        `shouldReturn` Right
          (JobClaim (LeaseToken 3) (AttemptCount 1) "payload")
      deadLetterJob store jobId (LeaseToken 3) Rejected
        `shouldReturn` Right ()
      claimJob store jobId (leaseWindow 60 70)
        `shouldReturn` Left (JobAlreadyDeadLettered jobId Rejected)
      history <- loadStream store jobIdentity
      fmap (fmap (.metadata.eventType)) history
        `shouldBe` Right
          [ "enqueued"
          , "claimed"
          , "deferred"
          , "claimed"
          , "retry-scheduled"
          , "claimed"
          , "dead-lettered"
          ]


jobRuntimeContract
  :: forall backend
   . ( Eq (BackendError backend)
     , JobStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
jobRuntimeContract label withStore = describe label do
  it "reconciles a later claim before an authorized resubmission" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store testAttemptLimit retrySchedule
          input = ProbeInput invocations
          job = ProbeJob SubmitSucceeds ReconcileAsNotSubmitted
      enqueueDurableJob runtime jobId job `shouldReturn` Right JobEnqueued
      ambiguous <- claimJob store jobId firstLease
      ambiguous `shouldSatisfy` isRight
      runJobOnce (Proxy @ProbeJob) runtime input jobId replacementLease
        `shouldReturn` Right (JobSucceeded "submitted")
      readIORef invocations
        `shouldReturn` [ReconcileInvoked, SubmitInvoked]

  it "defers an indeterminate reconciliation without resubmitting" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store testAttemptLimit retrySchedule
          input = ProbeInput invocations
          job = ProbeJob SubmitSucceeds ReconcileAsIndeterminate
      enqueueDurableJob runtime jobId job `shouldReturn` Right JobEnqueued
      ambiguous <- claimJob store jobId firstLease
      ambiguous `shouldSatisfy` isRight
      runJobOnce (Proxy @ProbeJob) runtime input jobId replacementLease
        `shouldReturn` Right (JobDeferred (LeaseInstant 40))
      readIORef invocations `shouldReturn` [ReconcileInvoked]
      claimJob store jobId (leaseWindow 30 50)
        `shouldReturn` Left (JobNotRunnable jobId (LeaseInstant 40))

  it "classifies terminal failures as retained dead letters" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store testAttemptLimit retrySchedule
          input = ProbeInput invocations
          job = ProbeJob SubmitTerminallyFails ReconcileAsNotSubmitted
      enqueueDurableJob runtime jobId job `shouldReturn` Right JobEnqueued
      runJobOnce (Proxy @ProbeJob) runtime input jobId firstLease
        `shouldReturn` Right (JobRejected ProbeFailure)
      claimJob store jobId replacementLease
        `shouldReturn` Left (JobAlreadyDeadLettered jobId Rejected)

  it "dead-letters a transient failure when its retry budget is exhausted" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store singleAttemptLimit retrySchedule
          input = ProbeInput invocations
          job = ProbeJob SubmitTransientlyFails ReconcileAsNotSubmitted
      enqueueDurableJob runtime jobId job `shouldReturn` Right JobEnqueued
      runJobOnce (Proxy @ProbeJob) runtime input jobId firstLease
        `shouldReturn` Right
          (JobRetriesExhausted (AttemptCount 1) ProbeFailure)
      claimJob store jobId replacementLease
        `shouldReturn` Left
          (JobAlreadyDeadLettered jobId RetriesExhausted)

  it "reconciles after a scheduled transient retry" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store testAttemptLimit retrySchedule
          input = ProbeInput invocations
          job = ProbeJob SubmitTransientlyFails ReconcileAsNotSubmitted
      enqueueDurableJob runtime jobId job `shouldReturn` Right JobEnqueued
      runJobOnce (Proxy @ProbeJob) runtime input jobId firstLease
        `shouldReturn` Right
          ( JobRetryScheduled
              (AttemptCount 1)
              (LeaseInstant 41)
              ProbeFailure
          )
      claimJob store jobId replacementLease
        `shouldReturn` Left (JobNotRunnable jobId (LeaseInstant 41))
      runJobOnce
        (Proxy @ProbeJob)
        runtime
        input
        jobId
        (leaseWindow 41 50)
        `shouldReturn` Right
          (JobRetriesExhausted (AttemptCount 2) ProbeFailure)
      readIORef invocations
        `shouldReturn` [SubmitInvoked, ReconcileInvoked, SubmitInvoked]

  it "dead-letters an undecodable payload without exposing it" $
    withStore \store -> do
      invocations <- newIORef []
      let runtime = mkJobRuntime store testAttemptLimit retrySchedule
          input = ProbeInput invocations
      enqueueJob store jobId "sensitive malformed payload"
        `shouldReturn` Right JobEnqueued
      runJobOnce (Proxy @ProbeJob) runtime input jobId firstLease
        `shouldReturn` Left
          (JobRunDecodeFailed jobId (DecodeCause "invalid stored job"))
      claimJob store jobId replacementLease
        `shouldReturn` Left (JobAlreadyDeadLettered jobId Undecodable)


reactorStoreContract
  :: forall backend
   . ( ReactorStore backend
     , Eq (BackendError backend)
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
reactorStoreContract label withStore = describe label do
  it "stores an outbox entry with its checkpoint" $ withStore \store -> do
    advanceReactor store firstReactorUpdate
      `shouldReturn` Right ReactorCommitted
    loadReactorCheckpoint store accountReactorName
      `shouldReturn` Right (Just (EventOffset 1))
    loadOutboxEntry store deliveryId
      `shouldReturn` Right (Just firstOutboxEntry)

  it "absorbs a repeated checkpoint without inserting another effect" $
    withStore \store -> do
      advanceReactor store firstReactorUpdate
        `shouldReturn` Right ReactorCommitted
      advanceReactor store repeatedReactorUpdate
        `shouldReturn` Right ReactorAlreadyCommitted
      loadOutboxEntry store secondDeliveryId `shouldReturn` Right Nothing

  it "rejects a skipped checkpoint without inserting its effect" $
    withStore \store -> do
      advanceReactor store skippedReactorUpdate
        `shouldReturn` Left
          ( ReactorSequenceMismatch
              accountReactorName
              (EventOffset 1)
              (EventOffset 2)
          )
      loadReactorCheckpoint store accountReactorName
        `shouldReturn` Right Nothing
      loadOutboxEntry store deliveryId `shouldReturn` Right Nothing

  it "rejects delivery identity reuse without advancing the checkpoint" $
    withStore \store -> do
      advanceReactor store firstReactorUpdate
        `shouldReturn` Right ReactorCommitted
      advanceReactor store conflictingReactorUpdate
        `shouldReturn` Left (ReactorDeliveryMismatch deliveryId)
      loadReactorCheckpoint store secondReactorName
        `shouldReturn` Right Nothing
      loadOutboxEntry store deliveryId
        `shouldReturn` Right (Just firstOutboxEntry)


reactorRunnerContract
  :: forall backend
   . ( Eq (BackendError backend)
     , ReactorStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
reactorRunnerContract label withStore = describe label do
  it "catches up once and resumes from its durable checkpoint" $
    withStore \store -> do
      initial <-
        validBatch
          (appendEvents accountKey NoStream (Opened 10 :| [Deposited 5]))
      commit store initial `shouldReturn` Right ()
      catchUpReactor store balanceReactor
        `shouldReturn` Right (EventOffset 2)
      loadOutboxEntry store deliveryId
        `shouldReturn` Right (Just firstOutboxEntry)
      catchUpReactor store balanceReactor
        `shouldReturn` Right (EventOffset 2)


outboxRuntimeContract
  :: forall backend
   . ( Eq (BackendError backend)
     , ReactorStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
outboxRuntimeContract label withStore = describe label do
  it "fences a delivery worker after its lease is replaced" $
    withStore \store -> do
      advanceReactor store firstReactorUpdate
        `shouldReturn` Right ReactorCommitted
      claimOutbox store deliveryId firstOutboxLease
        `shouldReturn` Right
          ( OutboxClaim
              (OutboxLeaseToken 1)
              (OutboxAttempt 0)
              firstOutboxEntry
          )
      claimOutbox store deliveryId (outboxLeaseWindow 15 25)
        `shouldReturn` Left
          (OutboxLeaseUnavailable deliveryId (OutboxInstant 20))
      claimOutbox store deliveryId secondOutboxLease
        `shouldReturn` Right
          ( OutboxClaim
              (OutboxLeaseToken 2)
              (OutboxAttempt 0)
              firstOutboxEntry
          )
      acknowledgeOutbox store deliveryId (OutboxLeaseToken 1)
        `shouldReturn` Left
          ( OutboxLeaseLost
              deliveryId
              (OutboxLeaseToken 1)
              (OutboxLeaseToken 2)
          )
      acknowledgeOutbox store deliveryId (OutboxLeaseToken 2)
        `shouldReturn` Right ()

  it "retries transient failures and then durably acknowledges success" $
    withStore \store -> do
      advanceReactor store firstReactorUpdate
        `shouldReturn` Right ReactorCommitted
      let runtime =
            mkOutboxRuntime store outboxAttemptLimit outboxRetrySchedule
      runOutboxOnce runtime deliveryId firstOutboxLease transientDelivery
        `shouldReturn` Right
          ( DeliveryRetryScheduled
              (OutboxAttempt 1)
              (OutboxInstant 30)
              ProbeFailure
          )
      runOutboxOnce runtime deliveryId secondOutboxLease successfulDelivery
        `shouldReturn` Right DeliverySucceeded
      loadOutboxStatus store deliveryId
        `shouldReturn` Right (Just OutboxDelivered)

  it "retains terminal failures as dead letters" $ withStore \store -> do
    advanceReactor store firstReactorUpdate
      `shouldReturn` Right ReactorCommitted
    let runtime =
          mkOutboxRuntime store outboxAttemptLimit outboxRetrySchedule
    runOutboxOnce runtime deliveryId firstOutboxLease terminalDelivery
      `shouldReturn` Right (DeliveryRejected ProbeFailure)
    loadOutboxStatus store deliveryId
      `shouldReturn` Right
        (Just (OutboxDeadLettered OutboxRejected))
    runOutboxOnce runtime deliveryId secondOutboxLease successfulDelivery
      `shouldReturn` Left
        (OutboxAlreadyDeadLettered deliveryId OutboxRejected)

  it "dead-letters a transient failure at its attempt limit" $
    withStore \store -> do
      advanceReactor store firstReactorUpdate
        `shouldReturn` Right ReactorCommitted
      let runtime =
            mkOutboxRuntime
              store
              singleOutboxAttemptLimit
              outboxRetrySchedule
      runOutboxOnce runtime deliveryId firstOutboxLease transientDelivery
        `shouldReturn` Right
          (DeliveryRetriesExhausted (OutboxAttempt 1) ProbeFailure)
      loadOutboxStatus store deliveryId
        `shouldReturn` Right
          (Just (OutboxDeadLettered OutboxRetriesExhausted))


storeContract
  :: forall backend
   . ( Eq (BackendError backend)
     , JobStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
storeContract label withBackend = describe label do
  it "initializes, transitions, and reloads an entity" $ withBackend \backend -> do
    let store = mkStore backend testLimits (pure jobId)
    loadEntity store accountKey `shouldReturn` Right Nothing
    executeCommand store accountKey (Open 10)
      `shouldReturn` Right (Account 10)
    executeCommand store accountKey (Deposit 5)
      `shouldReturn` Right (Account 15)
    loadEntity store accountKey `shouldReturn` Right (Just (Account 15))

  it "returns typed command and pre-application failures without mutation" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey InvalidDeposit
        `shouldReturn` Left (StoreDecisionRejected DepositBeforeOpen)
      loadStream backend accountIdentity `shouldReturn` Right []
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey (Open 20)
        `shouldReturn` Left (StoreCommandRejected AlreadyOpen)
      origin <- loadStream backend accountIdentity
      length <$> origin `shouldBe` Right 1

  it "atomically commits a dispatch intent and framework job enqueue" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey Notify
        `shouldReturn` Right (Account 10)
      origin <- loadStream backend accountIdentity
      length <$> origin `shouldBe` Right 2
      jobEvents <- loadStream backend jobIdentity
      fmap (fmap (.metadata)) jobEvents
        `shouldBe` Right
          [EventMetadata "job" "job-1" "enqueued" (EventVersion 1)]
      claimed <- claimJob backend jobId firstLease
      fmap (\(JobClaim token attempts _) -> (token, attempts)) claimed
        `shouldBe` Right (LeaseToken 1, AttemptCount 0)

  it "leaves the origin unchanged when the job stream conflicts" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey Notify
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey Notify
        `shouldReturn` Left
          ( StoreConcurrencyConflict
              (JobStreamConflict jobId)
              NoStream
              (At (StreamVersion 1))
          )
      origin <- loadStream backend accountIdentity
      length <$> origin `shouldBe` Right 2


snapshotContract
  :: forall backend
   . ( Eq (BackendError backend)
     , EventStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
snapshotContract label withBackend = describe label do
  it "stores a compatible snapshot and resumes from later events" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey (Deposit 5)
        `shouldReturn` Right (Account 15)
      snapshotEntity store accountKey RetainedHistory
        `shouldReturn` Right (Just (Account 15))
      loadedSnapshot <- loadSnapshot backend accountIdentity
      fmap (fmap snapshotStreamVersion) loadedSnapshot
        `shouldBe` Right (Just (StreamVersion 2))
      executeCommand store accountKey (Deposit 5)
        `shouldReturn` Right (Account 20)
      loadEntity store accountKey `shouldReturn` Right (Just (Account 20))

  it "ignores an incompatible retained snapshot and replays events" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
          upgradedStore = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      executeCommand store accountKey (Deposit 5)
        `shouldReturn` Right (Account 15)
      snapshotEntity store accountKey RetainedHistory
        `shouldReturn` Right (Just (Account 15))
      loadEntity upgradedStore accountV2Key
        `shouldReturn` Right (Just (AccountV2 15))
      discarded <- loadSnapshot backend accountIdentity
      fmap (fmap snapshotStreamVersion) discarded `shouldBe` Right Nothing

  it "fails closed for an incompatible compacted snapshot" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
          upgradedStore = mkStore backend testLimits (pure jobId)
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      snapshotEntity store accountKey CompactedHistory
        `shouldReturn` Right (Just (Account 10))
      loadEntity upgradedStore accountV2Key
        `shouldReturn` Left
          ( StoreSnapshotSchemaMismatch
              CompactedHistory
              (SchemaVersion 2)
              (SchemaVersion 1)
          )


schemaStoreContract
  :: forall backend
   . ( Eq (BackendError backend)
     , ProjectionStore backend
     , SchemaStore backend
     , Show (BackendError backend)
     )
  => [Char]
  -> (forall result. (backend -> IO result) -> IO result)
  -> Spec
schemaStoreContract label withBackend = describe label do
  it "preserves derived state when registered schemas remain current" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
      reconcileEntitySchema (Proxy @Account) backend
        `shouldReturn` Right SchemaRegistered
      reconcileProjectionSchema backend balanceProjection
        `shouldReturn` Right SchemaRegistered
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      snapshotEntity store accountKey RetainedHistory
        `shouldReturn` Right (Just (Account 10))
      catchUpProjection backend balanceProjection
        `shouldReturn` Right (BalanceView 10)
      reconcileEntitySchema (Proxy @Account) backend
        `shouldReturn` Right SchemaCurrent
      reconcileProjectionSchema backend balanceProjection
        `shouldReturn` Right SchemaCurrent
      loadedSnapshot <- loadSnapshot backend accountIdentity
      fmap isJust loadedSnapshot `shouldBe` Right True
      loadedProjection <- loadProjection backend balancesProjectionName
      fmap isJust loadedProjection `shouldBe` Right True

  it "invalidates only replayable derived state when schemas change" $
    withBackend \backend -> do
      let store = mkStore backend testLimits (pure jobId)
          upgradedStore = mkStore backend testLimits (pure jobId)
      reconcileEntitySchema (Proxy @Account) backend
        `shouldReturn` Right SchemaRegistered
      reconcileProjectionSchema backend balanceProjection
        `shouldReturn` Right SchemaRegistered
      executeCommand store accountKey (Open 10)
        `shouldReturn` Right (Account 10)
      snapshotEntity store accountKey RetainedHistory
        `shouldReturn` Right (Just (Account 10))
      catchUpProjection backend balanceProjection
        `shouldReturn` Right (BalanceView 10)
      reconcileEntitySchema (Proxy @AccountV2) backend
        `shouldReturn` Right (SchemaChanged (SchemaVersion 1))
      discardedSnapshot <- loadSnapshot backend accountIdentity
      fmap isNothing discardedSnapshot `shouldBe` Right True
      loadEntity upgradedStore accountV2Key
        `shouldReturn` Right (Just (AccountV2 10))
      reconcileProjectionSchema backend balanceProjectionV2
        `shouldReturn` Right (SchemaChanged (SchemaVersion 1))
      discardedProjection <- loadProjection backend balancesProjectionName
      fmap isNothing discardedProjection `shouldBe` Right True
      catchUpProjection backend balanceProjectionV2
        `shouldReturn` Right (BalanceView 10)


withMemoryStore :: (MemoryStore -> IO result) -> IO result
withMemoryStore action = newMemoryStore >>= action


withSQLiteStore :: (SQLiteStore -> IO result) -> IO result
withSQLiteStore = bracket acquire closeSQLiteStore
  where
    acquire = openSQLiteStore ":memory:" >>= either (panic . show) pure


validBatch :: StreamAppend -> IO CommitBatch
validBatch append = validBatchFrom (append :| [])


validBatchFrom :: NonEmpty StreamAppend -> IO CommitBatch
validBatchFrom appends = either (panic . show) pure (commitBatch testLimits appends)


testLimits :: CommitLimits
testLimits =
  mkCommitLimits
    (payloadLimit 1024)
    (batchLimit 16)


balancesProjectionName :: ProjectionName
balancesProjectionName =
  fromMaybe (panic "invalid projection name") (mkProjectionName "balances")


balanceProjection :: Projection Account BalanceView BalanceProjectionError
balanceProjection =
  Projection
    { name = balancesProjectionName
    , version = SchemaVersion 1
    , initial = BalanceView 0
    , apply = applyBalanceEvent
    , encode = LazyByteString.toStrict . Aeson.encode
    , decode =
        first (const (DecodeCause "invalid balance projection"))
          . Aeson.eitherDecodeStrict'
    }


balanceProjectionV2 :: Projection Account BalanceView BalanceProjectionError
balanceProjectionV2 = balanceProjection {version = SchemaVersion 2}


applyBalanceEvent
  :: BalanceView -> AccountEvent -> Either BalanceProjectionError BalanceView
applyBalanceEvent (BalanceView balance) event = case event of
  Opened amount -> Right (BalanceView (balance + amount))
  Deposited amount -> Right (BalanceView (balance + amount))
  NotificationQueued _ -> Right (BalanceView balance)


projectionCheckpoint :: ProjectionState -> EventOffset
projectionCheckpoint (ProjectionState checkpoint _) = checkpoint


deliveryId :: DeliveryId
deliveryId =
  fromMaybe (panic "invalid delivery id") (mkDeliveryId "delivery-1")


secondDeliveryId :: DeliveryId
secondDeliveryId =
  fromMaybe (panic "invalid delivery id") (mkDeliveryId "delivery-2")


accountReactorName :: ReactorName
accountReactorName =
  fromMaybe (panic "invalid reactor name") (mkReactorName "accounts")


balanceReactor :: Reactor Account BalanceReactorError
balanceReactor = Reactor accountReactorName react
  where
    react _ event = Right case event of
      Deposited _ -> Just firstOutboxEntry
      Opened _ -> Nothing
      NotificationQueued _ -> Nothing


secondReactorName :: ReactorName
secondReactorName =
  fromMaybe (panic "invalid reactor name") (mkReactorName "notifications")


firstOutboxEntry :: OutboxEntry
firstOutboxEntry = OutboxEntry deliveryId (CommandDelivery "open-account")


firstReactorUpdate :: ReactorUpdate
firstReactorUpdate =
  reactorUpdate accountReactorName (EventOffset 1) (Just firstOutboxEntry)


repeatedReactorUpdate :: ReactorUpdate
repeatedReactorUpdate =
  reactorUpdate
    accountReactorName
    (EventOffset 1)
    (Just (OutboxEntry secondDeliveryId (JobDispatch "notify")))


skippedReactorUpdate :: ReactorUpdate
skippedReactorUpdate =
  reactorUpdate accountReactorName (EventOffset 2) (Just firstOutboxEntry)


conflictingReactorUpdate :: ReactorUpdate
conflictingReactorUpdate =
  reactorUpdate
    secondReactorName
    (EventOffset 1)
    (Just (OutboxEntry deliveryId (CommandDelivery "different")))


jobId :: JobId
jobId = fromMaybe (panic "invalid job id") (mkJobId "job-1")


firstLease :: LeaseWindow
firstLease = leaseWindow 10 20


overlappingLease :: LeaseWindow
overlappingLease = leaseWindow 19 30


replacementLease :: LeaseWindow
replacementLease = leaseWindow 20 30


completedLease :: LeaseWindow
completedLease = leaseWindow 30 40


leaseWindow :: Word64 -> Word64 -> LeaseWindow
leaseWindow claimedAt expiresAt =
  fromMaybe
    (panic "invalid lease window")
    (mkLeaseWindow (LeaseInstant claimedAt) (LeaseInstant expiresAt))


testAttemptLimit :: AttemptLimit
testAttemptLimit =
  fromMaybe (panic "invalid attempt limit") (mkAttemptLimit 2)


singleAttemptLimit :: AttemptLimit
singleAttemptLimit =
  fromMaybe (panic "invalid attempt limit") (mkAttemptLimit 1)


retrySchedule :: AttemptCount -> LeaseInstant
retrySchedule (AttemptCount attempt) = LeaseInstant (40 + attempt)


outboxAttemptLimit :: OutboxAttemptLimit
outboxAttemptLimit =
  fromMaybe (panic "invalid outbox attempt limit") (mkOutboxAttemptLimit 2)


singleOutboxAttemptLimit :: OutboxAttemptLimit
singleOutboxAttemptLimit =
  fromMaybe (panic "invalid outbox attempt limit") (mkOutboxAttemptLimit 1)


outboxRetrySchedule :: OutboxAttempt -> OutboxInstant
outboxRetrySchedule (OutboxAttempt attempt) = OutboxInstant (29 + attempt)


firstOutboxLease :: OutboxLeaseWindow
firstOutboxLease = outboxLeaseWindow 10 20


secondOutboxLease :: OutboxLeaseWindow
secondOutboxLease = outboxLeaseWindow 30 40


outboxLeaseWindow :: Word64 -> Word64 -> OutboxLeaseWindow
outboxLeaseWindow claimedAt expiresAt =
  fromMaybe
    (panic "invalid outbox lease window")
    ( mkOutboxLeaseWindow
        (OutboxInstant claimedAt)
        (OutboxInstant expiresAt)
    )


transientDelivery
  :: DeliveryId
  -> OutboxPayload
  -> IO (Either (OutboxDeliveryFailure ProbeFailure) ())
transientDelivery _ _ = pure (Left (TransientDelivery ProbeFailure))


terminalDelivery
  :: DeliveryId
  -> OutboxPayload
  -> IO (Either (OutboxDeliveryFailure ProbeFailure) ())
terminalDelivery _ _ = pure (Left (TerminalDelivery ProbeFailure))


successfulDelivery
  :: DeliveryId
  -> OutboxPayload
  -> IO (Either (OutboxDeliveryFailure ProbeFailure) ())
successfulDelivery _ _ = pure (Right ())


recordProbeInvocation :: ProbeInput -> ProbeInvocation -> IO ()
recordProbeInvocation (ProbeInput invocations) invocation =
  modifyIORef' invocations (<> [invocation])


firstProjectionUpdate :: ProjectionUpdate
firstProjectionUpdate =
  projectionUpdate balancesProjectionName (EventOffset 1) "view-at-one"


repeatedProjectionUpdate :: ProjectionUpdate
repeatedProjectionUpdate =
  projectionUpdate balancesProjectionName (EventOffset 1) "replacement"


skippedProjectionUpdate :: ProjectionUpdate
skippedProjectionUpdate =
  projectionUpdate balancesProjectionName (EventOffset 2) "view-at-two"


payloadLimit :: Word64 -> PayloadLimit
payloadLimit value =
  fromMaybe (panic "invalid test payload limit") (mkPayloadLimit value)


batchLimit :: Word64 -> BatchLimit
batchLimit value =
  fromMaybe (panic "invalid test batch limit") (mkBatchLimit value)


isOversizedPayload :: Either CommitLimitViolation CommitBatch -> Bool
isOversizedPayload result = case result of
  Left (EventPayloadTooLarge streamIdentity size limit) ->
    streamIdentity == accountIdentity && size > 1 && limit == payloadLimit 1
  _ -> False


isOversizedBatch :: Either CommitLimitViolation CommitBatch -> Bool
isOversizedBatch result = case result of
  Left (BatchEventLimitExceeded count limit) ->
    count == 2 && limit == batchLimit 1
  _ -> False


isDuplicateStream :: Either CommitLimitViolation CommitBatch -> Bool
isDuplicateStream result = case result of
  Left (DuplicateStreamInBatch streamIdentity) -> streamIdentity == accountIdentity
  _ -> False


toV2 :: Account -> AccountV2
toV2 (Account balance) = AccountV2 balance


accountKey :: StreamKey Account
accountKey = streamKey (AccountId "account-1")


accountV2Key :: StreamKey AccountV2
accountV2Key = streamKey (AccountV2Id "account-1")


secondAccountKey :: StreamKey Account
secondAccountKey = streamKey (AccountId "account-2")


accountIdentity :: StreamIdentity
accountIdentity = StreamIdentity "account" "account-1"


secondAccountIdentity :: StreamIdentity
secondAccountIdentity = StreamIdentity "account" "account-2"


jobIdentity :: StreamIdentity
jobIdentity = StreamIdentity "job" "job-1"


accountMetadata :: EventMetadata
accountMetadata = EventMetadata "account" "account-1" "opened" (EventVersion 1)


stored :: Word64 -> AccountEvent -> StoredEvent
stored sequenceNumber event =
  StoredEvent
    (StreamPosition sequenceNumber)
    accountMetadata
      { eventType = Aggregate.eventType event
      , eventVersion = Aggregate.eventVersion event
      }
    (encodeEvent @Account event)
