module EventSorcery.Reactor (
  OutboxEntry (..),
  OutboxPayload (..),
  ReactorCommit (..),
  ReactorError (..),
  ReactorName,
  ReactorStore (..),
  ReactorUpdate,
  mkReactorName,
  reactorUpdate,
) where

import Data.Text qualified as Text
import EventSorcery.Reactor.Internal
import EventSorcery.Store.Internal
import Protolude


mkReactorName :: Text -> Maybe ReactorName
mkReactorName name
  | Text.null name = Nothing
  | otherwise = Just (ReactorName name)


reactorUpdate
  :: ReactorName -> EventOffset -> Maybe OutboxEntry -> ReactorUpdate
reactorUpdate name offset entry =
  ReactorUpdate (Unrestricted (name, offset, entry))
