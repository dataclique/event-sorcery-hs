module EventSorcery.Reactor.Internal (
  OutboxEntry (..),
  OutboxAttempt (..),
  OutboxAttemptLimit (..),
  OutboxClaim (..),
  OutboxDeadReason (..),
  OutboxDeliveryFailure (..),
  OutboxDeliveryResult (..),
  OutboxError (..),
  OutboxInstant (..),
  OutboxLeaseToken (..),
  OutboxLeaseWindow (..),
  OutboxPayload (..),
  OutboxRecord (..),
  OutboxRuntime (..),
  OutboxStatus (..),
  Reactor (..),
  ReactorCommit (..),
  ReactorContext (..),
  ReactorError (..),
  ReactorName (..),
  ReactorRunError (..),
  ReactorStore (..),
  ReactorUpdate (..),
  consumeReactorUpdate,
  decideAcknowledgeOutbox,
  decideClaimOutbox,
  decideDeadLetterOutbox,
  decideExhaustOutbox,
  decideReactorAdvance,
  decideRetryOutbox,
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


data OutboxRecord
  = OutboxRecord
      OutboxEntry
      OutboxStatus
      OutboxLeaseToken
      OutboxAttempt
  deriving stock (Eq, Show)


data OutboxStatus
  = OutboxReady
  | OutboxScheduled OutboxInstant
  | OutboxLeased OutboxInstant
  | OutboxDelivered
  | OutboxDeadLettered OutboxDeadReason
  deriving stock (Eq, Show)


data OutboxClaim
  = OutboxClaim
      OutboxLeaseToken
      OutboxAttempt
      OutboxEntry
  deriving stock (Eq, Show)


newtype OutboxInstant = OutboxInstant Word64
  deriving stock (Eq, Ord, Show)


newtype OutboxLeaseToken = OutboxLeaseToken Word64
  deriving stock (Eq, Ord, Show)


newtype OutboxAttempt = OutboxAttempt Word64
  deriving stock (Eq, Ord, Show)


newtype OutboxAttemptLimit = OutboxAttemptLimit Word64
  deriving stock (Eq, Ord, Show)


data OutboxLeaseWindow = OutboxLeaseWindow OutboxInstant OutboxInstant
  deriving stock (Eq, Show)


data OutboxDeadReason
  = OutboxRetriesExhausted
  | OutboxRejected
  deriving stock (Eq, Show)


data OutboxDeliveryFailure failure
  = TransientDelivery failure
  | TerminalDelivery failure
  deriving stock (Eq, Show)


data OutboxDeliveryResult failure
  = DeliverySucceeded
  | DeliveryRetryScheduled OutboxAttempt OutboxInstant failure
  | DeliveryRejected failure
  | DeliveryRetriesExhausted OutboxAttempt failure
  deriving stock (Eq, Show)


data OutboxRuntime backend
  = OutboxRuntime
      backend
      OutboxAttemptLimit
      (OutboxAttempt -> OutboxInstant)


data OutboxError backend
  = OutboxNotFound DeliveryId
  | OutboxNotRunnable DeliveryId OutboxInstant
  | OutboxLeaseUnavailable DeliveryId OutboxInstant
  | OutboxNotLeased DeliveryId
  | OutboxLeaseLost DeliveryId OutboxLeaseToken OutboxLeaseToken
  | OutboxAlreadyDelivered DeliveryId
  | OutboxAlreadyDeadLettered DeliveryId OutboxDeadReason
  | OutboxLeaseTokenExhausted DeliveryId
  | OutboxAttemptExhausted DeliveryId
  | OutboxBackendFailed (BackendError backend)


deriving stock instance
  Eq (BackendError backend) => Eq (OutboxError backend)


deriving stock instance
  Show (BackendError backend) => Show (OutboxError backend)


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
  loadOutboxStatus
    :: backend
    -> DeliveryId
    -> IO (Either (BackendError backend) (Maybe OutboxStatus))
  advanceReactor
    :: backend
    -> ReactorUpdate
    %1 -> IO (Either (ReactorError backend) ReactorCommit)
  claimOutbox
    :: backend
    -> DeliveryId
    -> OutboxLeaseWindow
    -> IO (Either (OutboxError backend) OutboxClaim)
  acknowledgeOutbox
    :: backend
    -> DeliveryId
    -> OutboxLeaseToken
    -> IO (Either (OutboxError backend) ())
  retryOutbox
    :: backend
    -> DeliveryId
    -> OutboxLeaseToken
    -> OutboxInstant
    -> IO (Either (OutboxError backend) OutboxAttempt)
  deadLetterOutbox
    :: backend
    -> DeliveryId
    -> OutboxLeaseToken
    -> IO (Either (OutboxError backend) ())
  exhaustOutbox
    :: backend
    -> DeliveryId
    -> OutboxLeaseToken
    -> IO (Either (OutboxError backend) OutboxAttempt)


consumeReactorUpdate
  :: ReactorUpdate
  %1 -> Unrestricted (ReactorName, EventOffset, Maybe OutboxEntry)
consumeReactorUpdate (ReactorUpdate update) = update


outboxDeliveryId :: OutboxEntry -> DeliveryId
outboxDeliveryId (OutboxEntry identifier _) = identifier


decideClaimOutbox
  :: DeliveryId
  -> OutboxLeaseWindow
  -> Maybe OutboxRecord
  -> Either (OutboxError backend) (OutboxClaim, OutboxRecord)
decideClaimOutbox identifier _ Nothing = Left (OutboxNotFound identifier)
decideClaimOutbox
  identifier
  window@(OutboxLeaseWindow claimedAt _)
  (Just record) = case record of
    OutboxRecord _ OutboxDelivered _ _ ->
      Left (OutboxAlreadyDelivered identifier)
    OutboxRecord _ (OutboxDeadLettered reason) _ _ ->
      Left (OutboxAlreadyDeadLettered identifier reason)
    OutboxRecord _ (OutboxScheduled runAt) _ _
      | claimedAt < runAt -> Left (OutboxNotRunnable identifier runAt)
    OutboxRecord _ (OutboxLeased expiresAt) _ _
      | claimedAt < expiresAt ->
          Left (OutboxLeaseUnavailable identifier expiresAt)
    _ -> claimAvailableOutbox identifier window record


decideAcknowledgeOutbox
  :: DeliveryId
  -> OutboxLeaseToken
  -> Maybe OutboxRecord
  -> Either (OutboxError backend) OutboxRecord
decideAcknowledgeOutbox identifier _ Nothing = Left (OutboxNotFound identifier)
decideAcknowledgeOutbox
  identifier
  requested
  (Just record@(OutboxRecord entry status held attempts))
    | requested /= held = Left (OutboxLeaseLost identifier requested held)
    | otherwise = case status of
        OutboxReady -> Left (OutboxNotLeased identifier)
        OutboxScheduled _ -> Left (OutboxNotLeased identifier)
        OutboxLeased _ ->
          Right (OutboxRecord entry OutboxDelivered held attempts)
        OutboxDelivered -> Right record
        OutboxDeadLettered reason ->
          Left (OutboxAlreadyDeadLettered identifier reason)


decideRetryOutbox
  :: DeliveryId
  -> OutboxLeaseToken
  -> OutboxInstant
  -> Maybe OutboxRecord
  -> Either (OutboxError backend) (OutboxAttempt, OutboxRecord)
decideRetryOutbox identifier token runAt current = do
  (entry, held, attempts) <- leasedOutbox identifier token current
  nextAttempt <- incrementOutboxAttempt identifier attempts
  pure
    ( nextAttempt
    , OutboxRecord entry (OutboxScheduled runAt) held nextAttempt
    )


decideDeadLetterOutbox
  :: DeliveryId
  -> OutboxLeaseToken
  -> Maybe OutboxRecord
  -> Either (OutboxError backend) OutboxRecord
decideDeadLetterOutbox identifier requested current = case current of
  Just record@(OutboxRecord _ (OutboxDeadLettered OutboxRejected) held _)
    | requested == held -> Right record
  _ -> do
    (entry, held, attempts) <- leasedOutbox identifier requested current
    pure
      ( OutboxRecord
          entry
          (OutboxDeadLettered OutboxRejected)
          held
          attempts
      )


decideExhaustOutbox
  :: DeliveryId
  -> OutboxLeaseToken
  -> Maybe OutboxRecord
  -> Either (OutboxError backend) (OutboxAttempt, OutboxRecord)
decideExhaustOutbox identifier token current = do
  (entry, held, attempts) <- leasedOutbox identifier token current
  nextAttempt <- incrementOutboxAttempt identifier attempts
  pure
    ( nextAttempt
    , OutboxRecord
        entry
        (OutboxDeadLettered OutboxRetriesExhausted)
        held
        nextAttempt
    )


claimAvailableOutbox
  :: DeliveryId
  -> OutboxLeaseWindow
  -> OutboxRecord
  -> Either (OutboxError backend) (OutboxClaim, OutboxRecord)
claimAvailableOutbox
  identifier
  (OutboxLeaseWindow _ expiresAt)
  (OutboxRecord entry _ token attempts) = do
    nextToken <- incrementOutboxLeaseToken identifier token
    pure
      ( OutboxClaim nextToken attempts entry
      , OutboxRecord entry (OutboxLeased expiresAt) nextToken attempts
      )


leasedOutbox
  :: DeliveryId
  -> OutboxLeaseToken
  -> Maybe OutboxRecord
  -> Either
       (OutboxError backend)
       (OutboxEntry, OutboxLeaseToken, OutboxAttempt)
leasedOutbox identifier _ Nothing = Left (OutboxNotFound identifier)
leasedOutbox
  identifier
  requested
  (Just (OutboxRecord entry status held attempts))
    | requested /= held = Left (OutboxLeaseLost identifier requested held)
    | otherwise = case status of
        OutboxReady -> Left (OutboxNotLeased identifier)
        OutboxScheduled _ -> Left (OutboxNotLeased identifier)
        OutboxLeased _ -> Right (entry, held, attempts)
        OutboxDelivered -> Left (OutboxAlreadyDelivered identifier)
        OutboxDeadLettered reason ->
          Left (OutboxAlreadyDeadLettered identifier reason)


incrementOutboxLeaseToken
  :: DeliveryId
  -> OutboxLeaseToken
  -> Either (OutboxError backend) OutboxLeaseToken
incrementOutboxLeaseToken identifier (OutboxLeaseToken token)
  | token == maxBound = Left (OutboxLeaseTokenExhausted identifier)
  | otherwise = Right (OutboxLeaseToken (token + 1))


incrementOutboxAttempt
  :: DeliveryId
  -> OutboxAttempt
  -> Either (OutboxError backend) OutboxAttempt
incrementOutboxAttempt identifier (OutboxAttempt attempts)
  | attempts == maxBound = Left (OutboxAttemptExhausted identifier)
  | otherwise = Right (OutboxAttempt (attempts + 1))


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
