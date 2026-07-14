module EventSorcery.Projection (
  Projection (..),
  ProjectionAdvance (..),
  ProjectionError (..),
  ProjectionName,
  ProjectionState (..),
  ProjectionStore (..),
  ProjectionUpdate,
  mkProjectionName,
  projectionUpdate,
) where

import Data.Text qualified as Text
import EventSorcery.Projection.Internal
import EventSorcery.Store.Internal
import Protolude


mkProjectionName :: Text -> Maybe ProjectionName
mkProjectionName name
  | Text.null name = Nothing
  | otherwise = Just (ProjectionName name)


projectionUpdate
  :: ProjectionName -> EventOffset -> ByteString -> ProjectionUpdate
projectionUpdate name offset view =
  ProjectionUpdate (Unrestricted (name, offset, view))
