module EventSorcery.Projection.Internal (
  Projection (..),
  ProjectionAdvance (..),
  ProjectionError (..),
  ProjectionName (..),
  ProjectionRunError (..),
  ProjectionState (..),
  ProjectionStore (..),
  ProjectionUpdate (..),
  consumeProjectionUpdate,
  decideProjectionAdvance,
) where

import EventSorcery.Aggregate
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


newtype ProjectionName = ProjectionName Text
  deriving stock (Eq, Ord, Show)


data Projection entity view projectionError = Projection
  { name :: ProjectionName
  , initial :: view
  , apply :: view -> Event entity -> Either projectionError view
  , encode :: view -> ByteString
  , decode :: ByteString -> Either DecodeCause view
  }


data ProjectionState = ProjectionState EventOffset ByteString
  deriving stock (Eq, Show)


data ProjectionUpdate where
  ProjectionUpdate
    :: Unrestricted (ProjectionName, EventOffset, ByteString)
    %1 -> ProjectionUpdate


data ProjectionAdvance
  = ProjectionAdvanced
  | ProjectionAlreadyApplied
  deriving stock (Eq, Show)


data ProjectionError backend
  = ProjectionSequenceMismatch ProjectionName EventOffset EventOffset
  | ProjectionOffsetExhausted ProjectionName EventOffset
  | ProjectionBackendFailed (BackendError backend)


deriving stock instance
  Eq (BackendError backend) => Eq (ProjectionError backend)


deriving stock instance
  Show (BackendError backend) => Show (ProjectionError backend)


data ProjectionRunError backend projectionError
  = ProjectionViewDecodeFailed ProjectionName DecodeCause
  | ProjectionEnvelopeDecodeFailed
      ProjectionName
      EventOffset
      StreamPosition
      DecodeCause
  | ProjectionEnvelopeMetadataMismatch
      ProjectionName
      EventOffset
      StreamPosition
      MetadataMismatch
  | ProjectionApplyFailed
      ProjectionName
      EventOffset
      StreamPosition
      projectionError
  | ProjectionCheckpointFailed (ProjectionError backend)
  | ProjectionReadFailed (BackendError backend)


deriving stock instance
  (Eq (BackendError backend), Eq projectionError)
  => Eq (ProjectionRunError backend projectionError)


deriving stock instance
  (Show (BackendError backend), Show projectionError)
  => Show (ProjectionRunError backend projectionError)


class EventStore backend => ProjectionStore backend where
  loadProjection
    :: backend
    -> ProjectionName
    -> IO (Either (BackendError backend) (Maybe ProjectionState))
  advanceProjection
    :: backend
    -> ProjectionUpdate
    %1 -> IO (Either (ProjectionError backend) ProjectionAdvance)
  resetProjection
    :: backend
    -> ProjectionName
    -> IO (Either (BackendError backend) ())


consumeProjectionUpdate
  :: ProjectionUpdate
  %1 -> Unrestricted (ProjectionName, EventOffset, ByteString)
consumeProjectionUpdate (ProjectionUpdate update) = update


decideProjectionAdvance
  :: ProjectionName
  -> EventOffset
  -> ByteString
  -> Maybe ProjectionState
  -> Either
       (ProjectionError backend)
       (ProjectionAdvance, Maybe ProjectionState)
decideProjectionAdvance name requested view current = case current of
  Just projectionState@(ProjectionState checkpoint _)
    | requested == checkpoint ->
        Right (ProjectionAlreadyApplied, Just projectionState)
  Just (ProjectionState checkpoint _) -> advanceFrom checkpoint
  Nothing -> advanceFrom (EventOffset 0)
  where
    advanceFrom checkpoint = case nextOffset checkpoint of
      Nothing -> Left (ProjectionOffsetExhausted name checkpoint)
      Just expected
        | requested == expected ->
            Right
              ( ProjectionAdvanced
              , Just (ProjectionState requested view)
              )
        | otherwise ->
            Left (ProjectionSequenceMismatch name expected requested)


nextOffset :: EventOffset -> Maybe EventOffset
nextOffset (EventOffset offset)
  | offset == maxBound = Nothing
  | otherwise = Just (EventOffset (offset + 1))
