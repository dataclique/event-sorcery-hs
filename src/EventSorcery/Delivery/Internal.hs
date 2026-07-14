module EventSorcery.Delivery.Internal (
  DeliveryCommit (..),
  DeliveryId (..),
  DeliveryStore (..),
) where

import EventSorcery.Store.Internal
import Protolude


newtype DeliveryId = DeliveryId Text
  deriving stock (Eq, Ord, Show)


data DeliveryCommit
  = DeliveryApplied
  | DeliveryAlreadyApplied
  deriving stock (Eq, Show)


class EventStore backend => DeliveryStore backend where
  commitDelivery
    :: backend
    -> DeliveryId
    -> CommitBatch
    %1 -> IO (Either (CommitError backend) DeliveryCommit)
