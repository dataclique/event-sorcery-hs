module EventSorcery.Delivery (
  DeliveryCommit (..),
  DeliveryId,
  DeliveryStore (..),
  mkDeliveryId,
) where

import Data.Text qualified as Text
import EventSorcery.Delivery.Internal
import Protolude


mkDeliveryId :: Text -> Maybe DeliveryId
mkDeliveryId identifier
  | Text.null identifier = Nothing
  | otherwise = Just (DeliveryId identifier)
