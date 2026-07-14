module EventSorcery.Schema (
  SchemaReconciliation (..),
  SchemaStore,
  reconcileEntitySchema,
  reconcileProjectionSchema,
) where

import EventSorcery.Aggregate
import EventSorcery.Projection.Internal
import EventSorcery.Schema.Internal
import EventSorcery.Store.Internal
import Protolude


reconcileEntitySchema
  :: (EventSourced entity, SchemaStore backend)
  => Proxy entity
  -> backend
  -> IO (Either (BackendError backend) SchemaReconciliation)
reconcileEntitySchema entity backend =
  reconcileSchema
    backend
    ( SchemaRegistration
        (AggregateSchema (aggregateType entity))
        (schemaVersion entity)
    )


reconcileProjectionSchema
  :: SchemaStore backend
  => backend
  -> Projection entity view projectionError
  -> IO (Either (BackendError backend) SchemaReconciliation)
reconcileProjectionSchema backend projection =
  reconcileSchema
    backend
    (SchemaRegistration (ProjectionSchema projection.name) projection.version)
