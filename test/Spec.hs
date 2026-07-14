module Main (main) where

import Conduit (runConduit, sinkList, (.|))
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as LazyByteString
import EventSorcery
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Backend.Memory
import EventSorcery.Backend.SQLite
import Protolude
import Test.Hspec


newtype AccountId = AccountId Text
  deriving stock (Eq, Show)


newtype Account = Account Word64
  deriving stock (Eq, Show)


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


newtype EmailJob = EmailJob Text


instance Job EmailJob where
  jobType _ = "email"
  encodeJob (EmailJob recipient) = encodeUtf8 recipient


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
  jobStoreContract "in-memory job store" withMemoryStore
  jobStoreContract "SQLite job store" withSQLiteStore
  reactorStoreContract "in-memory reactor store" withMemoryStore
  reactorStoreContract "SQLite reactor store" withSQLiteStore
  storeContract "in-memory typed store" withMemoryStore
  storeContract "SQLite typed store" withSQLiteStore


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
          (JobClaim (LeaseToken 1) (AttemptCount 1) "payload")
      claimJob store jobId overlappingLease
        `shouldReturn` Left
          (JobLeaseUnavailable jobId (LeaseInstant 20))
      claimJob store jobId replacementLease
        `shouldReturn` Right
          (JobClaim (LeaseToken 2) (AttemptCount 2) "payload")
      acknowledgeJob store jobId (LeaseToken 1)
        `shouldReturn` Left
          (JobLeaseLost jobId (LeaseToken 1) (LeaseToken 2))
      acknowledgeJob store jobId (LeaseToken 2) `shouldReturn` Right ()
      acknowledgeJob store jobId (LeaseToken 2) `shouldReturn` Right ()
      claimJob store jobId completedLease
        `shouldReturn` Left (JobAlreadyCompleted jobId)


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


storeContract
  :: forall backend
   . ( Eq (BackendError backend)
     , EventStore backend
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
    , initial = BalanceView 0
    , apply = applyBalanceEvent
    , encode = LazyByteString.toStrict . Aeson.encode
    , decode =
        first (const (DecodeCause "invalid balance projection"))
          . Aeson.eitherDecodeStrict'
    }


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


accountKey :: StreamKey Account
accountKey = streamKey (AccountId "account-1")


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
