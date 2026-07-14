module EventSorcery.Projection (
  Projection (..),
  ProjectionAdvance (..),
  ProjectionError (..),
  ProjectionName,
  ProjectionRunError (..),
  ProjectionState (..),
  ProjectionStore (..),
  ProjectionUpdate,
  mkProjectionName,
  projectionUpdate,
  catchUpProjection,
  rebuildProjection,
  rebuildProjections,
) where

import Conduit (foldMC, runConduit, transPipe, (.|))
import Data.Text qualified as Text
import EventSorcery.Aggregate
import EventSorcery.Projection.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


mkProjectionName :: Text -> Maybe ProjectionName
mkProjectionName name
  | Text.null name = Nothing
  | otherwise = Just (ProjectionName name)


projectionUpdate
  :: ProjectionName -> EventOffset -> ByteString -> ProjectionUpdate
projectionUpdate name offset view =
  ProjectionUpdate (Unrestricted (name, offset, view))


catchUpProjection
  :: (EventSourced entity, ProjectionStore backend)
  => backend
  -> Projection entity view projectionError
  -> IO (Either (ProjectionRunError backend projectionError) view)
catchUpProjection backend projection = runExceptT do
  stored <-
    liftIO (loadProjection backend projection.name)
      >>= either (throwError . ProjectionReadFailed) pure
  (checkpoint, view) <-
    either throwError pure (restoreProjection projection stored)
  runConduit
    ( transPipe
        (withExceptT ProjectionReadFailed)
        (streamEventsAfter backend checkpoint)
        .| foldMC (advanceEnvelope backend projection) view
    )


rebuildProjection
  :: (EventSourced entity, ProjectionStore backend)
  => backend
  -> Projection entity view projectionError
  -> IO (Either (ProjectionRunError backend projectionError) view)
rebuildProjection backend projection = do
  reset <- resetProjection backend projection.name
  case reset of
    Left failure -> pure (Left (ProjectionReadFailed failure))
    Right () -> catchUpProjection backend projection


rebuildProjections
  :: (EventSourced entity, ProjectionStore backend)
  => backend
  -> [Projection entity view projectionError]
  -> IO (Either (ProjectionRunError backend projectionError) [view])
rebuildProjections backend projections =
  runExceptT
    (traverse (ExceptT . rebuildProjection backend) projections)


restoreProjection
  :: Projection entity view projectionError
  -> Maybe ProjectionState
  -> Either
       (ProjectionRunError backend projectionError)
       (EventOffset, view)
restoreProjection projection stored = case stored of
  Nothing -> Right (EventOffset 0, projection.initial)
  Just (ProjectionState checkpoint payload) ->
    (checkpoint,)
      <$> first
        (ProjectionViewDecodeFailed projection.name)
        (projection.decode payload)


advanceEnvelope
  :: forall backend entity view projectionError
   . (EventSourced entity, ProjectionStore backend)
  => backend
  -> Projection entity view projectionError
  -> view
  -> StoredEnvelope
  -> ExceptT (ProjectionRunError backend projectionError) IO view
advanceEnvelope backend projection view envelope = do
  next <- either throwError pure (projectEnvelope projection view envelope)
  advanced <-
    liftIO
      ( advanceProjection
          backend
          (projectionUpdate projection.name offset (projection.encode next))
      )
  either (throwError . ProjectionCheckpointFailed) (const (pure next)) advanced
  where
    StoredEnvelope offset _ _ = envelope


projectEnvelope
  :: forall backend entity view projectionError
   . EventSourced entity
  => Projection entity view projectionError
  -> view
  -> StoredEnvelope
  -> Either (ProjectionRunError backend projectionError) view
projectEnvelope projection view (StoredEnvelope offset stream stored)
  | streamAggregateType stream /= expectedAggregateType = Right view
  | otherwise = do
      validateEnvelopeIdentity projection.name offset stream stored
      event <-
        first
          ( ProjectionEnvelopeDecodeFailed
              projection.name
              offset
              stored.position
          )
          (decodeEvent @entity stored.payload)
      validateProjectedEvent projection.name offset stored event
      first
        (ProjectionApplyFailed projection.name offset stored.position)
        (projection.apply view event)
  where
    expectedAggregateType = aggregateType (Proxy @entity)


validateEnvelopeIdentity
  :: ProjectionName
  -> EventOffset
  -> StreamIdentity
  -> StoredEvent
  -> Either (ProjectionRunError backend projectionError) ()
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
        . ProjectionEnvelopeMetadataMismatch
          name
          offset
          stored.position


validateProjectedEvent
  :: EventSourced entity
  => ProjectionName
  -> EventOffset
  -> StoredEvent
  -> Event entity
  -> Either (ProjectionRunError backend projectionError) ()
validateProjectedEvent name offset stored event
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
        . ProjectionEnvelopeMetadataMismatch
          name
          offset
          stored.position


streamAggregateType :: StreamIdentity -> Text
streamAggregateType (StreamIdentity aggregateName _) = aggregateName
