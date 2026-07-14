module Main (main) where

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
  deriving stock (Eq, Show)


data AccountEvent
  = Opened Word64
  | Deposited Word64
  deriving stock (Eq, Show, Generic)
  deriving anyclass (Aeson.FromJSON, Aeson.ToJSON)


data AccountCommandError = AlreadyOpen
  deriving stock (Eq, Show)


data AccountApplyError = DepositBeforeOpen
  deriving stock (Eq, Show)


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[]


  aggregateType _ = "account"
  encodeEntityId (AccountId identifier) = identifier
  eventType (Opened _) = "opened"
  eventType (Deposited _) = "deposited"
  eventVersion _ = EventVersion 1
  schemaVersion _ = SchemaVersion 1
  encodeEvent = LazyByteString.toStrict . Aeson.encode
  decodeEvent =
    first (const (DecodeCause "invalid account event")) . Aeson.eitherDecodeStrict'
  originate (Opened amount) = Right (Account amount)
  originate (Deposited _) = Left DepositBeforeOpen
  evolve (Account balance) (Deposited amount) = Right (Account (balance + amount))
  evolve account (Opened _) = Right account
  initialize (Open amount) = Right (Events (Opened amount :| []))
  initialize (Deposit _) = Left AlreadyOpen
  transition _ (Open _) = Left AlreadyOpen
  transition _ (Deposit amount) = Right (Events (Deposited amount :| []))


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

  eventStoreContract "in-memory event store" withMemoryStore
  eventStoreContract "SQLite event store" withSQLiteStore


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
