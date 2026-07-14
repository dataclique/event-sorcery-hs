module EventSorcery.Reactor.Internal (
  OutboxEntry (..),
  OutboxPayload (..),
  Reactor (..),
  ReactorCommit (..),
  ReactorContext (..),
  ReactorError (..),
  ReactorName (..),
  ReactorRunError (..),
  ReactorStore (..),
  ReactorUpdate (..),
  consumeReactorUpdate,
  decideReactorAdvance,
  outboxDeliveryId,
) where

import EventSorcery.Aggregate
import EventSorcery.Delivery.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype ReactorName = ReactorName Text
  deriving stock (Eq, Ord, Show)


data Reactor entity reactorError
  = Reactor
      ReactorName
      (ReactorContext -> Event entity -> Either reactorError (Maybe OutboxEntry))


data ReactorContext
  = ReactorContext
      EventOffset
      StreamIdentity
      StreamPosition
  deriving stock (Eq, Show)


data OutboxPayload
  = CommandDelivery ByteString
  | JobDispatch ByteString
  deriving stock (Eq, Show)


data OutboxEntry = OutboxEntry DeliveryId OutboxPayload
  deriving stock (Eq, Show)


data ReactorUpdate where
  ReactorUpdate
    :: Unrestricted (ReactorName, EventOffset, Maybe OutboxEntry)
    %1 -> ReactorUpdate


data ReactorCommit
  = ReactorCommitted
  | ReactorAlreadyCommitted
  deriving stock (Eq, Show)


data ReactorError backend
  = ReactorSequenceMismatch ReactorName EventOffset EventOffset
  | ReactorOffsetExhausted ReactorName EventOffset
  | ReactorDeliveryMismatch DeliveryId
  | ReactorBackendFailed (BackendError backend)


data ReactorRunError backend reactorError
  = ReactorEnvelopeDecodeFailed
      ReactorName
      EventOffset
      StreamPosition
      DecodeCause
  | ReactorEnvelopeMetadataMismatch
      ReactorName
      EventOffset
      StreamPosition
      MetadataMismatch
  | ReactorReactionFailed
      ReactorName
      EventOffset
      StreamPosition
      reactorError
  | ReactorCheckpointFailed (ReactorError backend)
  | ReactorReadFailed (BackendError backend)


deriving stock instance
  (Eq (BackendError backend), Eq reactorError)
  => Eq (ReactorRunError backend reactorError)


deriving stock instance
  (Show (BackendError backend), Show reactorError)
  => Show (ReactorRunError backend reactorError)


deriving stock instance
  Eq (BackendError backend) => Eq (ReactorError backend)


deriving stock instance
  Show (BackendError backend) => Show (ReactorError backend)


class EventStore backend => ReactorStore backend where
  loadReactorCheckpoint
    :: backend
    -> ReactorName
    -> IO (Either (BackendError backend) (Maybe EventOffset))
  loadOutboxEntry
    :: backend
    -> DeliveryId
    -> IO (Either (BackendError backend) (Maybe OutboxEntry))
  advanceReactor
    :: backend
    -> ReactorUpdate
    %1 -> IO (Either (ReactorError backend) ReactorCommit)


consumeReactorUpdate
  :: ReactorUpdate
  %1 -> Unrestricted (ReactorName, EventOffset, Maybe OutboxEntry)
consumeReactorUpdate (ReactorUpdate update) = update


outboxDeliveryId :: OutboxEntry -> DeliveryId
outboxDeliveryId (OutboxEntry identifier _) = identifier


decideReactorAdvance
  :: ReactorName
  -> EventOffset
  -> Maybe OutboxEntry
  -> Maybe EventOffset
  -> Maybe OutboxEntry
  -> Either
       (ReactorError backend)
       (ReactorCommit, Maybe EventOffset, Maybe OutboxEntry)
decideReactorAdvance name requested proposed current existing =
  case current of
    Just checkpoint
      | requested == checkpoint ->
          Right (ReactorAlreadyCommitted, current, Nothing)
    _ -> advanceFrom (fromMaybe (EventOffset 0) current)
  where
    advanceFrom checkpoint = case nextOffset checkpoint of
      Nothing -> Left (ReactorOffsetExhausted name checkpoint)
      Just expected
        | requested /= expected ->
            Left (ReactorSequenceMismatch name expected requested)
        | otherwise -> do
            insertion <- decideOutbox proposed existing
            pure (ReactorCommitted, Just requested, insertion)


decideOutbox
  :: Maybe OutboxEntry
  -> Maybe OutboxEntry
  -> Either (ReactorError backend) (Maybe OutboxEntry)
decideOutbox Nothing _ = Right Nothing
decideOutbox proposed@(Just _) Nothing = Right proposed
decideOutbox (Just proposed) (Just existing)
  | proposed == existing = Right Nothing
  | otherwise = Left (ReactorDeliveryMismatch (outboxDeliveryId proposed))


nextOffset :: EventOffset -> Maybe EventOffset
nextOffset (EventOffset offset)
  | offset == maxBound = Nothing
  | otherwise = Just (EventOffset (offset + 1))
