module Main (main) where

import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as LazyByteString
import EventSorcery
import EventSorcery.Aggregate qualified as Aggregate
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


accountKey :: StreamKey Account
accountKey = streamKey (AccountId "account-1")


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
