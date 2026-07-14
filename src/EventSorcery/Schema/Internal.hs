module EventSorcery.Schema.Internal (
  SchemaReconciliation (..),
  SchemaRegistration (..),
  SchemaInvalidation (..),
  SchemaStore (..),
  SchemaTarget (..),
  decideSchemaReconciliation,
) where

import EventSorcery.Aggregate
import EventSorcery.Projection.Internal (ProjectionName)
import EventSorcery.Store.Internal
import Protolude


data SchemaTarget
  = AggregateSchema Text
  | ProjectionSchema ProjectionName
  deriving stock (Eq, Ord, Show)


data SchemaRegistration
  = SchemaRegistration SchemaTarget SchemaVersion
  deriving stock (Eq, Show)


data SchemaReconciliation
  = SchemaRegistered
  | SchemaCurrent
  | SchemaChanged SchemaVersion
  deriving stock (Eq, Show)


data SchemaInvalidation
  = PreserveDerivedState
  | InvalidateDerivedState
  deriving stock (Eq, Show)


class EventStore backend => SchemaStore backend where
  reconcileSchema
    :: backend
    -> SchemaRegistration
    -> IO (Either (BackendError backend) SchemaReconciliation)


decideSchemaReconciliation
  :: SchemaVersion
  -> Maybe SchemaVersion
  -> (SchemaReconciliation, SchemaInvalidation)
decideSchemaReconciliation _ Nothing =
  (SchemaRegistered, PreserveDerivedState)
decideSchemaReconciliation requested (Just current)
  | requested == current = (SchemaCurrent, PreserveDerivedState)
  | otherwise = (SchemaChanged current, InvalidateDerivedState)
