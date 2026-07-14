module EventSorcery.Backend.SQLite (
  SQLiteError (..),
  SQLiteFailure (..),
  SQLiteStore,
  closeSQLiteStore,
  openSQLiteStore,
) where

import Conduit qualified
import Data.List.NonEmpty qualified as NonEmpty
import Database.SQLite.Simple (
  Connection,
  Only (..),
  ResultError,
  SQLError,
  close,
  execute,
  execute_,
  open,
  query,
  withImmediateTransaction,
 )
import Database.SQLite.Simple.FromRow (FromRow (..), field)
import EventSorcery.Aggregate (EventVersion (..), SchemaVersion (..))
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Delivery.Internal
import EventSorcery.Job.Internal
import EventSorcery.Projection.Internal
import EventSorcery.Reactor.Internal
import EventSorcery.Schema.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


data SQLiteStore = SQLiteStore Connection (MVar ())


data SQLiteError
  = SQLiteOpenFailed SQLiteFailure
  | SQLiteReadFailed SQLiteFailure
  | SQLiteCommitFailed SQLiteFailure
  | SQLiteStoredDataInvalid
  | SQLiteSnapshotVersionInvalid
  | SQLiteJobPayloadMismatch JobId
  deriving stock (Eq, Show)


data SQLiteFailure
  = SQLiteEngineFailure SQLError
  | SQLiteResultFailure ResultError
  deriving stock (Eq, Show)


data EventRow = EventRow Word64 Text Word16 ByteString


data EnvelopeRow
  = EnvelopeRow
      Word64
      Text
      Text
      Word64
      Text
      Word16
      ByteString


data ProjectionRow = ProjectionRow Word64 ByteString


data SnapshotRow = SnapshotRow Word64 Word16 Text ByteString


data JobRow = JobRow ByteString Text Word64 (Maybe Word64) Word64


data OutboxRow = OutboxRow Text ByteString


instance FromRow EventRow where
  fromRow = EventRow <$> field <*> field <*> field <*> field


instance FromRow EnvelopeRow where
  fromRow =
    EnvelopeRow
      <$> field
      <*> field
      <*> field
      <*> field
      <*> field
      <*> field
      <*> field


instance FromRow ProjectionRow where
  fromRow = ProjectionRow <$> field <*> field


instance FromRow SnapshotRow where
  fromRow = SnapshotRow <$> field <*> field <*> field <*> field


instance FromRow JobRow where
  fromRow = JobRow <$> field <*> field <*> field <*> field <*> field


instance FromRow OutboxRow where
  fromRow = OutboxRow <$> field <*> field


openSQLiteStore :: FilePath -> IO (Either SQLiteError SQLiteStore)
openSQLiteStore path = do
  opened <- trySQLite (open path)
  case opened of
    Left failure -> pure (Left (SQLiteOpenFailed failure))
    Right connection -> do
      initialized <- trySQLite (initializeConnection connection)
      case initialized of
        Left failure -> close connection $> Left (SQLiteOpenFailed failure)
        Right () -> do
          writeLock <- newMVar ()
          pure (Right (SQLiteStore connection writeLock))


closeSQLiteStore :: SQLiteStore -> IO ()
closeSQLiteStore (SQLiteStore connection _) = close connection


instance EventStore SQLiteStore where
  type BackendError SQLiteStore = SQLiteError


  loadStream (SQLiteStore connection _) streamIdentity = do
    loaded <- trySQLite (loadRows connection streamIdentity)
    pure (first SQLiteReadFailed loaded)


  loadStreamAfter (SQLiteStore connection _) streamIdentity version = do
    loaded <- trySQLite (loadRowsAfter connection streamIdentity version)
    pure (first SQLiteReadFailed loaded)


  loadSnapshot (SQLiteStore connection _) streamIdentity = do
    loaded <- trySQLite (loadSQLiteSnapshot connection streamIdentity)
    pure case loaded of
      Left failure -> Left (SQLiteReadFailed failure)
      Right decoded -> decoded


  discardSnapshot store@(SQLiteStore connection _) (StreamIdentity aggregateName identifier) = do
    discarded <-
      trySQLite
        ( withSQLiteWrite
            store
            ( execute
                connection
                "DELETE FROM snapshots WHERE aggregate_type = ? AND aggregate_id = ?"
                (aggregateName, identifier)
            )
        )
    pure (first SQLiteCommitFailed discarded)


  storeSnapshot = storeSQLiteSnapshot


  streamEventsAfter (SQLiteStore connection _) = streamRows connection


  commit = commitSQLite


instance ProjectionStore SQLiteStore where
  loadProjection (SQLiteStore connection _) name = do
    loaded <- trySQLite (loadProjectionState connection name)
    pure (first SQLiteReadFailed loaded)


  advanceProjection store@(SQLiteStore connection _) update =
    case consumeProjectionUpdate update of
      Unrestricted (name, offset, view) -> do
        advanced <-
          trySQLite
            ( withSQLiteTransaction
                store
                (advanceProjectionTransaction connection name offset view)
            )
        pure case advanced of
          Left failure ->
            Left (ProjectionBackendFailed (SQLiteCommitFailed failure))
          Right result -> result


  resetProjection store@(SQLiteStore connection _) (ProjectionName name) = do
    reset <-
      trySQLite
        ( withSQLiteWrite
            store
            (execute connection "DELETE FROM projections WHERE name = ?" (Only name))
        )
    pure (first SQLiteCommitFailed reset)


instance DeliveryStore SQLiteStore where
  loadDeliveryReceipt (SQLiteStore connection _) delivery = do
    loaded <- trySQLite (deliveryRecorded connection delivery)
    pure (first SQLiteReadFailed loaded)


  commitDelivery store@(SQLiteStore connection _) delivery batch =
    case consumeCommitBatch batch of
      Unrestricted appends -> do
        committed <-
          trySQLite
            ( withSQLiteTransaction
                store
                (commitDeliveryTransaction connection delivery appends)
            )
        pure case committed of
          Left failure -> Left (BackendFailed (SQLiteCommitFailed failure))
          Right result -> result


instance JobStore SQLiteStore where
  enqueueJob store@(SQLiteStore connection _) identifier payload = do
    enqueued <-
      trySQLite
        ( withSQLiteTransaction
            store
            (enqueueJobTransaction connection identifier payload)
        )
    pure (sqliteJobResult enqueued)


  claimJob store@(SQLiteStore connection _) identifier window = do
    claimed <-
      trySQLite
        ( withSQLiteTransaction
            store
            (claimJobTransaction connection identifier window)
        )
    pure (sqliteJobResult claimed)


  acknowledgeJob store@(SQLiteStore connection _) identifier token = do
    acknowledged <-
      trySQLite
        ( withSQLiteTransaction
            store
            (acknowledgeJobTransaction connection identifier token)
        )
    pure (sqliteJobResult acknowledged)


  retryJob store@(SQLiteStore connection _) identifier token runAt = do
    retried <-
      trySQLite
        ( withSQLiteTransaction
            store
            ( transitionJobRecord
                connection
                identifier
                JobRetryScheduledEvent
                (decideRetry identifier token runAt)
            )
        )
    pure (sqliteJobResult retried)


  deferJob store@(SQLiteStore connection _) identifier token runAt = do
    deferred <-
      trySQLite
        ( withSQLiteTransaction
            store
            ( transitionJobRecord
                connection
                identifier
                JobDeferredEvent
                (fmap ((),) . decideDefer identifier token runAt)
            )
        )
    pure (sqliteJobResult deferred)


  deadLetterJob store@(SQLiteStore connection _) identifier token reason = do
    deadLettered <-
      trySQLite
        ( withSQLiteTransaction
            store
            ( transitionJobRecord
                connection
                identifier
                JobDeadLetteredEvent
                (fmap ((),) . decideDeadLetter identifier token reason)
            )
        )
    pure (sqliteJobResult deadLettered)


  exhaustJob store@(SQLiteStore connection _) identifier token = do
    exhausted <-
      trySQLite
        ( withSQLiteTransaction
            store
            ( transitionJobRecord
                connection
                identifier
                JobDeadLetteredEvent
                (decideExhaust identifier token)
            )
        )
    pure (sqliteJobResult exhausted)


instance ReactorStore SQLiteStore where
  loadReactorCheckpoint (SQLiteStore connection _) name = do
    loaded <- trySQLite (loadSQLiteReactorCheckpoint connection name)
    pure (first SQLiteReadFailed loaded)


  loadOutboxEntry (SQLiteStore connection _) identifier = do
    loaded <- trySQLite (loadSQLiteOutboxEntry connection identifier)
    pure case loaded of
      Left failure -> Left (SQLiteReadFailed failure)
      Right decoded -> decoded


  advanceReactor store@(SQLiteStore connection _) update =
    case consumeReactorUpdate update of
      Unrestricted (name, offset, proposed) -> do
        advanced <-
          trySQLite
            ( withSQLiteTransaction
                store
                (advanceReactorTransaction connection name offset proposed)
            )
        pure case advanced of
          Left failure ->
            Left (ReactorBackendFailed (SQLiteCommitFailed failure))
          Right result -> result


instance SchemaStore SQLiteStore where
  reconcileSchema store@(SQLiteStore connection _) registration = do
    reconciled <-
      trySQLite
        ( withSQLiteTransaction
            store
            (reconcileSchemaTransaction connection registration)
        )
    pure (first SQLiteCommitFailed reconciled)


commitSQLite
  :: SQLiteStore
  -> CommitBatch
  %1 -> IO (Either (CommitError SQLiteStore) ())
commitSQLite store batch = case consumeCommitBatch batch of
  Unrestricted appends -> commitAppends store appends


commitAppends
  :: SQLiteStore
  -> NonEmpty StreamAppend
  -> IO (Either (CommitError SQLiteStore) ())
commitAppends store@(SQLiteStore connection _) appends = do
  committed <-
    trySQLite
      (withSQLiteTransaction store (commitTransaction connection appends))
  pure case committed of
    Left failure -> Left (BackendFailed (SQLiteCommitFailed failure))
    Right result -> result


trySQLite :: IO result -> IO (Either SQLiteFailure result)
trySQLite action = do
  attempted <- try @SQLError (try @ResultError action)
  pure case attempted of
    Left failure -> Left (SQLiteEngineFailure failure)
    Right (Left failure) -> Left (SQLiteResultFailure failure)
    Right (Right result) -> Right result


withSQLiteWrite :: SQLiteStore -> IO result -> IO result
withSQLiteWrite (SQLiteStore _ writeLock) action =
  withMVar writeLock (const action)


withSQLiteTransaction :: SQLiteStore -> IO result -> IO result
withSQLiteTransaction store@(SQLiteStore connection _) action =
  withSQLiteWrite store (withImmediateTransaction connection action)


initializeConnection :: Connection -> IO ()
initializeConnection connection = do
  execute_ connection "PRAGMA busy_timeout = 5000"
  migrate connection


migrate :: Connection -> IO ()
migrate connection = do
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS events (
      global_offset INTEGER PRIMARY KEY AUTOINCREMENT,
      aggregate_type TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      event_version INTEGER NOT NULL,
      payload BLOB NOT NULL,
      UNIQUE (aggregate_type, aggregate_id, sequence)
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS reactors (
      name TEXT PRIMARY KEY,
      event_offset INTEGER NOT NULL
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS outbox (
      delivery_id TEXT PRIMARY KEY,
      payload_type TEXT NOT NULL,
      payload BLOB NOT NULL
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS jobs (
      job_id TEXT PRIMARY KEY,
      payload BLOB NOT NULL,
      status TEXT NOT NULL,
      lease_token INTEGER NOT NULL,
      lease_expires INTEGER,
      attempts INTEGER NOT NULL
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS delivery_receipts (
      delivery_id TEXT PRIMARY KEY
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS projections (
      name TEXT PRIMARY KEY,
      event_offset INTEGER NOT NULL,
      view BLOB NOT NULL
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS snapshots (
      aggregate_type TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      schema_version INTEGER NOT NULL,
      history TEXT NOT NULL,
      payload BLOB NOT NULL,
      PRIMARY KEY (aggregate_type, aggregate_id)
    )
    """
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS schemas (
      kind TEXT NOT NULL CHECK (kind IN ('aggregate', 'projection')),
      name TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      PRIMARY KEY (kind, name)
    )
    """


loadRows :: Connection -> StreamIdentity -> IO [StoredEvent]
loadRows connection (StreamIdentity aggregateName identifier) = do
  rows <-
    query
      connection
      """
      SELECT sequence, event_type, event_version, payload
      FROM events
      WHERE aggregate_type = ? AND aggregate_id = ?
      ORDER BY sequence
      """
      (aggregateName, identifier)
  pure (toStored <$> rows)
  where
    toStored (EventRow position eventName version payload) =
      StoredEvent
        (StreamPosition position)
        (EventMetadata aggregateName identifier eventName (EventVersion version))
        payload


loadRowsAfter
  :: Connection -> StreamIdentity -> StreamVersion -> IO [StoredEvent]
loadRowsAfter
  connection
  (StreamIdentity aggregateName identifier)
  (StreamVersion version) = do
    rows <-
      query
        connection
        """
        SELECT sequence, event_type, event_version, payload
        FROM events
        WHERE aggregate_type = ? AND aggregate_id = ? AND sequence > ?
        ORDER BY sequence
        """
        (aggregateName, identifier, version)
    pure (toStored <$> rows)
    where
      toStored (EventRow position eventName eventSchema payload) =
        StoredEvent
          (StreamPosition position)
          ( EventMetadata
              aggregateName
              identifier
              eventName
              (EventVersion eventSchema)
          )
          payload


loadSQLiteSnapshot
  :: Connection
  -> StreamIdentity
  -> IO (Either SQLiteError (Maybe StoredSnapshot))
loadSQLiteSnapshot
  connection
  (StreamIdentity aggregateName identifier) = do
    rows <-
      query
        connection
        """
        SELECT sequence, schema_version, history, payload
        FROM snapshots
        WHERE aggregate_type = ? AND aggregate_id = ?
        """
        (aggregateName, identifier)
    pure case rows of
      row : _ -> Just <$> decodeSnapshotRow row
      [] -> Right Nothing


decodeSnapshotRow :: SnapshotRow -> Either SQLiteError StoredSnapshot
decodeSnapshotRow (SnapshotRow version schema history payload) = do
  decodedHistory <- case history of
    "retained" -> Right RetainedHistory
    "compacted" -> Right CompactedHistory
    _ -> Left SQLiteStoredDataInvalid
  pure
    ( StoredSnapshot
        (StreamVersion version)
        (SchemaVersion schema)
        decodedHistory
        payload
    )


storeSQLiteSnapshot
  :: SQLiteStore
  -> SnapshotWrite
  %1 -> IO (Either SQLiteError ())
storeSQLiteSnapshot store@(SQLiteStore connection _) write =
  case consumeSnapshotWrite write of
    Unrestricted snapshot -> do
      stored <-
        trySQLite
          ( withSQLiteTransaction
              store
              (storeSnapshotTransaction connection snapshot)
          )
      pure case stored of
        Left failure -> Left (SQLiteCommitFailed failure)
        Right True -> Right ()
        Right False -> Left SQLiteSnapshotVersionInvalid


storeSnapshotTransaction
  :: Connection -> (StreamIdentity, StoredSnapshot) -> IO Bool
storeSnapshotTransaction connection (streamIdentity, snapshot) = do
  actual <- currentSQLiteVersion connection streamIdentity
  if snapshotWithinVersion snapshot actual
    then insertSnapshot connection streamIdentity snapshot $> True
    else pure False


snapshotWithinVersion :: StoredSnapshot -> ExpectedVersion -> Bool
snapshotWithinVersion _ NoStream = False
snapshotWithinVersion
  (StoredSnapshot proposed _ _ _)
  (At actual) = proposed <= actual


insertSnapshot :: Connection -> StreamIdentity -> StoredSnapshot -> IO ()
insertSnapshot
  connection
  (StreamIdentity aggregateName identifier)
  (StoredSnapshot (StreamVersion version) (SchemaVersion schema) history payload) =
    execute
      connection
      """
      INSERT INTO snapshots (
        aggregate_type,
        aggregate_id,
        sequence,
        schema_version,
        history,
        payload
      )
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
        sequence = excluded.sequence,
        schema_version = excluded.schema_version,
        history = excluded.history,
        payload = excluded.payload
      WHERE excluded.sequence >= snapshots.sequence
      """
      ( aggregateName
      , identifier
      , version
      , schema
      , snapshotHistoryText history
      , payload
      )


snapshotHistoryText :: SnapshotHistory -> Text
snapshotHistoryText RetainedHistory = "retained"
snapshotHistoryText CompactedHistory = "compacted"


streamRows
  :: Connection
  -> EventOffset
  -> Conduit.ConduitT
       ()
       StoredEnvelope
       (ExceptT SQLiteError IO)
       ()
streamRows connection offset = do
  loaded <- liftIO (trySQLite (loadEnvelopeRows connection offset))
  rows <- either (throwError . SQLiteReadFailed) pure loaded
  case NonEmpty.nonEmpty rows of
    Nothing -> pure ()
    Just page -> do
      let envelopes = toEnvelope <$> page
      Conduit.yieldMany (NonEmpty.toList envelopes)
      streamRows connection (envelopeOffset (NonEmpty.last envelopes))


loadEnvelopeRows :: Connection -> EventOffset -> IO [EnvelopeRow]
loadEnvelopeRows connection (EventOffset offset) =
  query
    connection
    """
    SELECT
      global_offset,
      aggregate_type,
      aggregate_id,
      sequence,
      event_type,
      event_version,
      payload
    FROM events
    WHERE global_offset > ?
    ORDER BY global_offset
    LIMIT ?
    """
    (offset, projectionPageSize)


projectionPageSize :: Word16
projectionPageSize = 256


toEnvelope :: EnvelopeRow -> StoredEnvelope
toEnvelope
  (EnvelopeRow offset aggregateName identifier position eventName version payload) =
    StoredEnvelope
      (EventOffset offset)
      (StreamIdentity aggregateName identifier)
      ( StoredEvent
          (StreamPosition position)
          ( EventMetadata
              aggregateName
              identifier
              eventName
              (EventVersion version)
          )
          payload
      )


envelopeOffset :: StoredEnvelope -> EventOffset
envelopeOffset (StoredEnvelope offset _ _) = offset


loadProjectionState
  :: Connection -> ProjectionName -> IO (Maybe ProjectionState)
loadProjectionState connection (ProjectionName name) = do
  rows <-
    query
      connection
      """
      SELECT event_offset, view
      FROM projections
      WHERE name = ?
      """
      (Only name)
  pure case rows of
    ProjectionRow offset view : _ ->
      Just (ProjectionState (EventOffset offset) view)
    [] -> Nothing


advanceProjectionTransaction
  :: Connection
  -> ProjectionName
  -> EventOffset
  -> ByteString
  -> IO (Either (ProjectionError SQLiteStore) ProjectionAdvance)
advanceProjectionTransaction connection name offset view = do
  current <- loadProjectionState connection name
  case decideProjectionAdvance name offset view current of
    Left failure -> pure (Left failure)
    Right (advance, next) -> do
      traverse_ (storeProjectionState connection name) next
      pure (Right advance)


storeProjectionState
  :: Connection -> ProjectionName -> ProjectionState -> IO ()
storeProjectionState
  connection
  (ProjectionName name)
  (ProjectionState (EventOffset offset) view) =
    execute
      connection
      """
      INSERT INTO projections (name, event_offset, view)
      VALUES (?, ?, ?)
      ON CONFLICT (name) DO UPDATE SET
        event_offset = excluded.event_offset,
        view = excluded.view
      """
      (name, offset, view)


reconcileSchemaTransaction
  :: Connection
  -> SchemaRegistration
  -> IO SchemaReconciliation
reconcileSchemaTransaction
  connection
  (SchemaRegistration target requested) = do
    current <- loadSchemaVersion connection target
    let (result, invalidation) = decideSchemaReconciliation requested current
    case invalidation of
      PreserveDerivedState -> pure ()
      InvalidateDerivedState -> invalidateSQLiteSchema connection target
    unless (result == SchemaCurrent) do
      storeSchemaVersion connection target requested
    pure result


loadSchemaVersion
  :: Connection -> SchemaTarget -> IO (Maybe SchemaVersion)
loadSchemaVersion connection target = do
  rows <-
    query
      connection
      """
      SELECT schema_version
      FROM schemas
      WHERE kind = ? AND name = ?
      """
      (schemaTargetParts target)
  pure case rows of
    Only version : _ -> Just (SchemaVersion version)
    [] -> Nothing


storeSchemaVersion
  :: Connection -> SchemaTarget -> SchemaVersion -> IO ()
storeSchemaVersion connection target (SchemaVersion version) =
  execute
    connection
    """
    INSERT INTO schemas (kind, name, schema_version)
    VALUES (?, ?, ?)
    ON CONFLICT (kind, name) DO UPDATE SET
      schema_version = excluded.schema_version
    """
    (kind, name, version)
  where
    (kind, name) = schemaTargetParts target


invalidateSQLiteSchema :: Connection -> SchemaTarget -> IO ()
invalidateSQLiteSchema connection target = case target of
  AggregateSchema aggregateName ->
    execute
      connection
      "DELETE FROM snapshots WHERE aggregate_type = ?"
      (Only aggregateName)
  ProjectionSchema (ProjectionName name) ->
    execute
      connection
      "DELETE FROM projections WHERE name = ?"
      (Only name)


schemaTargetParts :: SchemaTarget -> (Text, Text)
schemaTargetParts target = case target of
  AggregateSchema name -> ("aggregate", name)
  ProjectionSchema (ProjectionName name) -> ("projection", name)


enqueueJobTransaction
  :: Connection
  -> JobId
  -> ByteString
  -> IO (Either (JobError SQLiteStore) JobEnqueue)
enqueueJobTransaction connection identifier payload = do
  current <- loadJobRecord connection identifier
  case current >>= decideEnqueue identifier payload of
    Left failure -> pure (Left failure)
    Right (JobAlreadyEnqueued, _) -> pure (Right JobAlreadyEnqueued)
    Right (JobEnqueued, next) -> do
      storeSQLiteJobEvent
        connection
        identifier
        JobEnqueuedEvent
        next
      pure (Right JobEnqueued)


claimJobTransaction
  :: Connection
  -> JobId
  -> LeaseWindow
  -> IO (Either (JobError SQLiteStore) JobClaim)
claimJobTransaction connection identifier window = do
  current <- loadJobRecord connection identifier
  case current >>= decideClaim identifier window of
    Left failure -> pure (Left failure)
    Right (claim, next) -> do
      storeSQLiteJobEvent connection identifier JobClaimedEvent next
      pure (Right claim)


acknowledgeJobTransaction
  :: Connection
  -> JobId
  -> LeaseToken
  -> IO (Either (JobError SQLiteStore) ())
acknowledgeJobTransaction connection identifier token = do
  current <- loadJobRecord connection identifier
  case current >>= decideAcknowledge identifier token of
    Left failure -> pure (Left failure)
    Right next -> do
      storeChangedSQLiteJobEvent
        connection
        identifier
        JobSucceededEvent
        current
        next
      pure (Right ())


transitionJobRecord
  :: Connection
  -> JobId
  -> JobLifecycleEvent
  -> ( Maybe JobRecord
       -> Either (JobError SQLiteStore) (result, JobRecord)
     )
  -> IO (Either (JobError SQLiteStore) result)
transitionJobRecord connection identifier lifecycle decide = do
  current <- loadJobRecord connection identifier
  case current >>= decide of
    Left failure -> pure (Left failure)
    Right (result, next) -> do
      storeChangedSQLiteJobEvent
        connection
        identifier
        lifecycle
        current
        next
      pure (Right result)


storeChangedSQLiteJobEvent
  :: Connection
  -> JobId
  -> JobLifecycleEvent
  -> Either (JobError SQLiteStore) (Maybe JobRecord)
  -> JobRecord
  -> IO ()
storeChangedSQLiteJobEvent connection identifier lifecycle current next =
  unless (current == Right (Just next)) do
    storeSQLiteJobEvent connection identifier lifecycle next


storeSQLiteJobEvent
  :: Connection
  -> JobId
  -> JobLifecycleEvent
  -> JobRecord
  -> IO ()
storeSQLiteJobEvent connection identifier lifecycle record = do
  let jobIdentity = StreamIdentity "job" (Aggregate.jobIdText identifier)
  actual <- currentSQLiteVersion connection jobIdentity
  insertValidatedAppend
    connection
    ( ValidatedAppend
        actual
        (jobEventAppend identifier actual lifecycle record)
    )
  storeJobRecord connection identifier record


loadJobRecord
  :: Connection
  -> JobId
  -> IO (Either (JobError SQLiteStore) (Maybe JobRecord))
loadJobRecord connection identifier = do
  rows <-
    query
      connection
      """
      SELECT payload, status, lease_token, lease_expires, attempts
      FROM jobs
      WHERE job_id = ?
      """
      (Only (Aggregate.jobIdText identifier))
  pure case rows of
    row : _ -> Just <$> decodeJobRow row
    [] -> Right Nothing


decodeJobRow :: JobRow -> Either (JobError SQLiteStore) JobRecord
decodeJobRow (JobRow payload status token expires attempts) = do
  decodedStatus <- case (status, expires) of
    ("ready", Nothing) -> Right JobReady
    ("scheduled", Just runAt) -> Right (JobScheduled (LeaseInstant runAt))
    ("leased", Just expiresAt) -> Right (JobLeased (LeaseInstant expiresAt))
    ("completed", Nothing) -> Right JobCompleted
    ("dead-retries-exhausted", Nothing) ->
      Right (JobDeadLettered RetriesExhausted)
    ("dead-rejected", Nothing) -> Right (JobDeadLettered Rejected)
    ("dead-undecodable", Nothing) -> Right (JobDeadLettered Undecodable)
    ("dead-abandoned", Nothing) -> Right (JobDeadLettered Abandoned)
    _ -> Left (JobBackendFailed SQLiteStoredDataInvalid)
  pure
    ( JobRecord
        payload
        decodedStatus
        (LeaseToken token)
        (AttemptCount attempts)
    )


storeJobRecord :: Connection -> JobId -> JobRecord -> IO ()
storeJobRecord
  connection
  identifier
  (JobRecord payload status (LeaseToken token) (AttemptCount attempts)) =
    execute
      connection
      """
      INSERT INTO jobs (
        job_id,
        payload,
        status,
        lease_token,
        lease_expires,
        attempts
      )
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT (job_id) DO UPDATE SET
        payload = excluded.payload,
        status = excluded.status,
        lease_token = excluded.lease_token,
        lease_expires = excluded.lease_expires,
        attempts = excluded.attempts
      """
      ( Aggregate.jobIdText identifier
      , payload
      , statusText status
      , token
      , leaseExpiry status
      , attempts
      )


statusText :: JobStatus -> Text
statusText status = case status of
  JobReady -> "ready"
  JobScheduled _ -> "scheduled"
  JobLeased _ -> "leased"
  JobCompleted -> "completed"
  JobDeadLettered RetriesExhausted -> "dead-retries-exhausted"
  JobDeadLettered Rejected -> "dead-rejected"
  JobDeadLettered Undecodable -> "dead-undecodable"
  JobDeadLettered Abandoned -> "dead-abandoned"


leaseExpiry :: JobStatus -> Maybe Word64
leaseExpiry status = case status of
  JobScheduled (LeaseInstant runAt) -> Just runAt
  JobLeased (LeaseInstant expiresAt) -> Just expiresAt
  JobReady -> Nothing
  JobCompleted -> Nothing
  JobDeadLettered _ -> Nothing


sqliteJobResult
  :: Either SQLiteFailure (Either (JobError SQLiteStore) result)
  -> Either (JobError SQLiteStore) result
sqliteJobResult =
  either (Left . JobBackendFailed . SQLiteCommitFailed) identity


advanceReactorTransaction
  :: Connection
  -> ReactorName
  -> EventOffset
  -> Maybe OutboxEntry
  -> IO (Either (ReactorError SQLiteStore) ReactorCommit)
advanceReactorTransaction connection name offset proposed = do
  checkpoint <- loadSQLiteReactorCheckpoint connection name
  existing <- loadProposedOutbox connection proposed
  case existing >>= decideReactorAdvance name offset proposed checkpoint of
    Left failure -> pure (Left failure)
    Right (result, nextCheckpoint, insertion) -> do
      traverse_ (storeOutboxEntry connection) insertion
      traverse_ (storeReactorCheckpoint connection name) nextCheckpoint
      pure (Right result)


loadSQLiteReactorCheckpoint
  :: Connection -> ReactorName -> IO (Maybe EventOffset)
loadSQLiteReactorCheckpoint connection (ReactorName name) = do
  rows <-
    query
      connection
      """
      SELECT event_offset
      FROM reactors
      WHERE name = ?
      """
      (Only name)
  pure case rows of
    Only offset : _ -> Just (EventOffset offset)
    [] -> Nothing


loadProposedOutbox
  :: Connection
  -> Maybe OutboxEntry
  -> IO (Either (ReactorError SQLiteStore) (Maybe OutboxEntry))
loadProposedOutbox _ Nothing = pure (Right Nothing)
loadProposedOutbox connection (Just entry) =
  first ReactorBackendFailed
    <$> loadSQLiteOutboxEntry connection (outboxDeliveryId entry)


loadSQLiteOutboxEntry
  :: Connection
  -> DeliveryId
  -> IO (Either SQLiteError (Maybe OutboxEntry))
loadSQLiteOutboxEntry connection identifier@(DeliveryId delivery) = do
  rows <-
    query
      connection
      """
      SELECT payload_type, payload
      FROM outbox
      WHERE delivery_id = ?
      """
      (Only delivery)
  pure case rows of
    row : _ -> Just . OutboxEntry identifier <$> decodeOutboxRow row
    [] -> Right Nothing


decodeOutboxRow :: OutboxRow -> Either SQLiteError OutboxPayload
decodeOutboxRow (OutboxRow payloadType payload) = case payloadType of
  "command" -> Right (CommandDelivery payload)
  "job" -> Right (JobDispatch payload)
  _ -> Left SQLiteStoredDataInvalid


storeOutboxEntry :: Connection -> OutboxEntry -> IO ()
storeOutboxEntry
  connection
  (OutboxEntry (DeliveryId delivery) payload) =
    execute
      connection
      """
      INSERT INTO outbox (delivery_id, payload_type, payload)
      VALUES (?, ?, ?)
      """
      (delivery, outboxPayloadType payload, outboxPayloadBytes payload)


outboxPayloadType :: OutboxPayload -> Text
outboxPayloadType payload = case payload of
  CommandDelivery _ -> "command"
  JobDispatch _ -> "job"


outboxPayloadBytes :: OutboxPayload -> ByteString
outboxPayloadBytes payload = case payload of
  CommandDelivery bytes -> bytes
  JobDispatch bytes -> bytes


storeReactorCheckpoint
  :: Connection -> ReactorName -> EventOffset -> IO ()
storeReactorCheckpoint
  connection
  (ReactorName name)
  (EventOffset offset) =
    execute
      connection
      """
      INSERT INTO reactors (name, event_offset)
      VALUES (?, ?)
      ON CONFLICT (name) DO UPDATE SET
        event_offset = excluded.event_offset
      """
      (name, offset)


commitDeliveryTransaction
  :: Connection
  -> DeliveryId
  -> NonEmpty StreamAppend
  -> IO (Either (CommitError SQLiteStore) DeliveryCommit)
commitDeliveryTransaction connection delivery appends = do
  recorded <- deliveryRecorded connection delivery
  if recorded
    then pure (Right DeliveryAlreadyApplied)
    else do
      validated <- validateAppends connection appends
      case validated of
        Left failure -> pure (Left failure)
        Right validatedAppends -> do
          traverse_ (insertValidatedAppend connection) validatedAppends
          insertDeliveryReceipt connection delivery
          pure (Right DeliveryApplied)


deliveryRecorded :: Connection -> DeliveryId -> IO Bool
deliveryRecorded connection (DeliveryId delivery) = do
  rows <-
    query
      connection
      """
      SELECT 1
      FROM delivery_receipts
      WHERE delivery_id = ?
      LIMIT 1
      """
      (Only delivery)
  pure case rows of
    (_ :: Only Word8) : _ -> True
    [] -> False


insertDeliveryReceipt :: Connection -> DeliveryId -> IO ()
insertDeliveryReceipt connection (DeliveryId delivery) =
  execute
    connection
    "INSERT INTO delivery_receipts (delivery_id) VALUES (?)"
    (Only delivery)


commitTransaction
  :: Connection -> NonEmpty StreamAppend -> IO (Either (CommitError SQLiteStore) ())
commitTransaction connection appends = do
  validated <- validateAppends connection appends
  case validated of
    Left failure -> pure (Left failure)
    Right validatedAppends ->
      traverse_ (insertValidatedAppend connection) validatedAppends $> Right ()


data ValidatedAppend = ValidatedAppend ExpectedVersion StreamAppend


validateAppends
  :: Connection
  -> NonEmpty StreamAppend
  -> IO
       ( Either
           (CommitError SQLiteStore)
           (NonEmpty ValidatedAppend)
       )
validateAppends connection = runExceptT . traverse validate
  where
    validate append = do
      actual <-
        liftIO
          (currentSQLiteVersion connection (streamAppendIdentity append))
      let expected = streamAppendExpectedVersion append
      if expected == actual
        then do
          validatedJob <- liftIO (validateSQLiteJobSeed connection append)
          either throwError pure validatedJob
          pure (ValidatedAppend actual append)
        else
          throwError
            (ConcurrencyConflict (streamAppendIdentity append) expected actual)


currentSQLiteVersion :: Connection -> StreamIdentity -> IO ExpectedVersion
currentSQLiteVersion connection (StreamIdentity aggregateName identifier) = do
  rows <-
    query
      connection
      """
      SELECT MAX(sequence)
      FROM events
      WHERE aggregate_type = ? AND aggregate_id = ?
      """
      (aggregateName, identifier)
  pure case rows of
    [Only (Just version :: Maybe Word64)] -> At (StreamVersion version)
    _ -> NoStream


insertValidatedAppend :: Connection -> ValidatedAppend -> IO ()
insertValidatedAppend connection (ValidatedAppend actual append) = do
  let firstPosition = case actual of
        NoStream -> 1
        At (StreamVersion version) -> version + 1
  traverse_ (uncurry insertEvent) (zip [firstPosition ..] events)
  traverse_ (uncurry (seedSQLiteJob connection)) (frameworkJobSeed append)
  where
    StreamIdentity aggregateName identifier =
      streamAppendIdentity append
    events = NonEmpty.toList (streamAppendEvents append)
    insertEvent position (ProposedEvent metadata payload) =
      execute
        connection
        """
        INSERT INTO events (
          aggregate_type,
          aggregate_id,
          sequence,
          event_type,
          event_version,
          payload
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        ( aggregateName
        , identifier
        , position
        , metadata.eventType
        , case metadata.eventVersion of EventVersion version -> version
        , payload
        )


validateSQLiteJobSeed
  :: Connection
  -> StreamAppend
  -> IO (Either (CommitError SQLiteStore) ())
validateSQLiteJobSeed connection append = case frameworkJobSeed append of
  Nothing -> pure (Right ())
  Just (_, Left _) -> pure (Left (BackendFailed SQLiteStoredDataInvalid))
  Just (identifier, Right record) -> do
    current <- loadJobRecord connection identifier
    pure case current of
      Left _ -> Left (BackendFailed SQLiteStoredDataInvalid)
      Right Nothing -> Right ()
      Right (Just stored)
        | stored == record -> Right ()
        | otherwise -> Left (BackendFailed (SQLiteJobPayloadMismatch identifier))


seedSQLiteJob
  :: Connection
  -> JobId
  -> Either Aggregate.DecodeCause JobRecord
  -> IO ()
seedSQLiteJob _ _ (Left _) = pure ()
seedSQLiteJob connection identifier (Right record) =
  storeJobRecord connection identifier record
