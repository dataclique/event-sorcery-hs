module EventSorcery.Reactor (
  OutboxEntry (..),
  OutboxAttempt (..),
  OutboxAttemptLimit,
  OutboxClaim (..),
  OutboxDeadReason (..),
  OutboxDeliveryFailure (..),
  OutboxDeliveryResult (..),
  OutboxError (..),
  OutboxInstant (..),
  OutboxLeaseToken (..),
  OutboxLeaseWindow,
  OutboxPayload (..),
  OutboxRuntime,
  OutboxStatus (..),
  Reactor (..),
  ReactorCommit (..),
  ReactorContext,
  ReactorError (..),
  ReactorName,
  ReactorRunError (..),
  ReactorStore (..),
  ReactorUpdate,
  catchUpReactor,
  mkOutboxAttemptLimit,
  mkOutboxLeaseWindow,
  mkOutboxRuntime,
  mkReactorName,
  reactorContextOffset,
  reactorContextPosition,
  reactorContextStream,
  reactorUpdate,
  runOutboxOnce,
) where

import Conduit (foldMC, runConduit, transPipe, (.|))
import Data.Text qualified as Text
import EventSorcery.Aggregate
import EventSorcery.Delivery.Internal (DeliveryId)
import EventSorcery.Reactor.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


mkReactorName :: Text -> Maybe ReactorName
mkReactorName name
  | Text.null name = Nothing
  | otherwise = Just (ReactorName name)


reactorUpdate
  :: ReactorName -> EventOffset -> Maybe OutboxEntry -> ReactorUpdate
reactorUpdate name offset entry =
  ReactorUpdate (Unrestricted (name, offset, entry))


mkOutboxAttemptLimit :: Word64 -> Maybe OutboxAttemptLimit
mkOutboxAttemptLimit 0 = Nothing
mkOutboxAttemptLimit attempts = Just (OutboxAttemptLimit attempts)


mkOutboxLeaseWindow
  :: OutboxInstant -> OutboxInstant -> Maybe OutboxLeaseWindow
mkOutboxLeaseWindow claimedAt expiresAt
  | claimedAt < expiresAt = Just (OutboxLeaseWindow claimedAt expiresAt)
  | otherwise = Nothing


mkOutboxRuntime
  :: backend
  -> OutboxAttemptLimit
  -> (OutboxAttempt -> OutboxInstant)
  -> OutboxRuntime backend
mkOutboxRuntime = OutboxRuntime


runOutboxOnce
  :: ReactorStore backend
  => OutboxRuntime backend
  -> DeliveryId
  -> OutboxLeaseWindow
  -> ( DeliveryId
       -> OutboxPayload
       -> IO (Either (OutboxDeliveryFailure failure) ())
     )
  -> IO
       ( Either
           (OutboxError backend)
           (OutboxDeliveryResult failure)
       )
runOutboxOnce runtime@(OutboxRuntime backend _ _) identifier window deliver = do
  claimed <- claimOutbox backend identifier window
  case claimed of
    Left failure -> pure (Left failure)
    Right (OutboxClaim token attempts (OutboxEntry delivery payload)) -> do
      delivered <- deliver delivery payload
      case delivered of
        Right () -> do
          acknowledged <- acknowledgeOutbox backend identifier token
          pure (DeliverySucceeded <$ acknowledged)
        Left (TerminalDelivery failure) -> do
          deadLettered <- deadLetterOutbox backend identifier token
          pure (DeliveryRejected failure <$ deadLettered)
        Left (TransientDelivery failure) ->
          persistTransientDelivery
            runtime
            identifier
            token
            attempts
            failure


persistTransientDelivery
  :: ReactorStore backend
  => OutboxRuntime backend
  -> DeliveryId
  -> OutboxLeaseToken
  -> OutboxAttempt
  -> failure
  -> IO
       ( Either
           (OutboxError backend)
           (OutboxDeliveryResult failure)
       )
persistTransientDelivery
  (OutboxRuntime backend limit schedule)
  identifier
  token
  attempts
  failure = case nextOutboxAttempt attempts of
    Nothing -> pure (Left (OutboxAttemptExhausted identifier))
    Just next
      | reachedOutboxAttemptLimit limit next -> do
          exhausted <- exhaustOutbox backend identifier token
          pure (DeliveryRetriesExhausted <$> exhausted <*> pure failure)
      | otherwise -> do
          let runAt = schedule next
          retried <- retryOutbox backend identifier token runAt
          pure
            ( (\actual -> DeliveryRetryScheduled actual runAt failure)
                <$> retried
            )


nextOutboxAttempt :: OutboxAttempt -> Maybe OutboxAttempt
nextOutboxAttempt (OutboxAttempt attempts)
  | attempts == maxBound = Nothing
  | otherwise = Just (OutboxAttempt (attempts + 1))


reachedOutboxAttemptLimit
  :: OutboxAttemptLimit -> OutboxAttempt -> Bool
reachedOutboxAttemptLimit
  (OutboxAttemptLimit limit)
  (OutboxAttempt attempts) = attempts >= limit


reactorContextOffset :: ReactorContext -> EventOffset
reactorContextOffset (ReactorContext offset _ _) = offset


reactorContextStream :: ReactorContext -> StreamIdentity
reactorContextStream (ReactorContext _ stream _) = stream


reactorContextPosition :: ReactorContext -> StreamPosition
reactorContextPosition (ReactorContext _ _ position) = position


catchUpReactor
  :: forall backend entity reactorError
   . (EventSourced entity, ReactorStore backend)
  => backend
  -> Reactor entity reactorError
  -> IO (Either (ReactorRunError backend reactorError) EventOffset)
catchUpReactor backend reactor@(Reactor name _) = runExceptT do
  checkpoint <-
    liftIO (loadReactorCheckpoint backend name)
      >>= either (throwError . ReactorReadFailed) (pure . fromMaybe (EventOffset 0))
  runConduit
    ( transPipe
        (withExceptT ReactorReadFailed)
        (streamEventsAfter backend checkpoint)
        .| foldMC (advanceEnvelope backend reactor) checkpoint
    )


advanceEnvelope
  :: forall backend entity reactorError
   . (EventSourced entity, ReactorStore backend)
  => backend
  -> Reactor entity reactorError
  -> EventOffset
  -> StoredEnvelope
  -> ExceptT (ReactorRunError backend reactorError) IO EventOffset
advanceEnvelope backend reactor@(Reactor name _) _ envelope = do
  proposed <- either throwError pure (reactEnvelope reactor envelope)
  advanced <-
    liftIO
      ( advanceReactor
          backend
          (reactorUpdate name offset proposed)
      )
  either (throwError . ReactorCheckpointFailed) (const (pure offset)) advanced
  where
    StoredEnvelope offset _ _ = envelope


reactEnvelope
  :: forall backend entity reactorError
   . EventSourced entity
  => Reactor entity reactorError
  -> StoredEnvelope
  -> Either
       (ReactorRunError backend reactorError)
       (Maybe OutboxEntry)
reactEnvelope (Reactor name react) (StoredEnvelope offset stream stored)
  | streamAggregateType stream /= expectedAggregateType = Right Nothing
  | otherwise = do
      validateEnvelopeIdentity name offset stream stored
      event <-
        first
          ( ReactorEnvelopeDecodeFailed
              name
              offset
              stored.position
          )
          (decodeEvent @entity stored.payload)
      validateReactedEvent name offset stored event
      first
        (ReactorReactionFailed name offset stored.position)
        (react (ReactorContext offset stream stored.position) event)
  where
    expectedAggregateType = aggregateType (Proxy @entity)


validateEnvelopeIdentity
  :: ReactorName
  -> EventOffset
  -> StreamIdentity
  -> StoredEvent
  -> Either (ReactorRunError backend reactorError) ()
validateEnvelopeIdentity name offset (StreamIdentity aggregateName identifier) stored
  | stored.metadata.aggregateType /= aggregateName =
      mismatch
        ( AggregateTypeMismatch
            aggregateName
            stored.metadata.aggregateType
        )
  | stored.metadata.aggregateId /= identifier =
      mismatch (AggregateIdMismatch identifier stored.metadata.aggregateId)
  | otherwise = Right ()
  where
    mismatch =
      Left
        . ReactorEnvelopeMetadataMismatch
          name
          offset
          stored.position


validateReactedEvent
  :: EventSourced entity
  => ReactorName
  -> EventOffset
  -> StoredEvent
  -> Event entity
  -> Either (ReactorRunError backend reactorError) ()
validateReactedEvent name offset stored event
  | stored.metadata.eventType /= expectedType =
      mismatch (EventTypeMismatch expectedType stored.metadata.eventType)
  | stored.metadata.eventVersion /= expectedVersion =
      mismatch
        ( EventVersionMismatch
            expectedVersion
            stored.metadata.eventVersion
        )
  | otherwise = Right ()
  where
    expectedType = eventType event
    expectedVersion = eventVersion event
    mismatch =
      Left
        . ReactorEnvelopeMetadataMismatch
          name
          offset
          stored.position


streamAggregateType :: StreamIdentity -> Text
streamAggregateType (StreamIdentity aggregateName _) = aggregateName
