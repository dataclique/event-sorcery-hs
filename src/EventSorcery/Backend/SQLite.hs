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
import EventSorcery.Aggregate (EventVersion (..))
import EventSorcery.Delivery.Internal
import EventSorcery.Projection.Internal
import EventSorcery.Store.Internal
import EventSorcery.Stream
import Protolude


data SQLiteStore = SQLiteStore Connection (MVar ())


data SQLiteError
  = SQLiteOpenFailed SQLiteFailure
  | SQLiteReadFailed SQLiteFailure
  | SQLiteCommitFailed SQLiteFailure
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


  streamEventsAfter (SQLiteStore connection) = streamRows connection


  commit = commitSQLite


instance ProjectionStore SQLiteStore where
  loadProjection (SQLiteStore connection) name = do
    loaded <- try @SQLError (loadProjectionState connection name)
    pure (first (const SQLiteReadFailed) loaded)


  advanceProjection (SQLiteStore connection) update =
    case consumeProjectionUpdate update of
      Unrestricted (name, offset, view) -> do
        advanced <-
          try @SQLError
            ( withTransaction
                connection
                (advanceProjectionTransaction connection name offset view)
            )
        pure case advanced of
          Left _ -> Left (ProjectionBackendFailed SQLiteCommitFailed)
          Right result -> result


  resetProjection (SQLiteStore connection) (ProjectionName name) = do
    reset <-
      try @SQLError
        (execute connection "DELETE FROM projections WHERE name = ?" (Only name))
    pure (first (const SQLiteCommitFailed) reset)


instance DeliveryStore SQLiteStore where
  commitDelivery (SQLiteStore connection) delivery batch =
    case consumeCommitBatch batch of
      Unrestricted appends -> do
        committed <-
          try @SQLError
            ( withTransaction
                connection
                (commitDeliveryTransaction connection delivery appends)
            )
        pure case committed of
          Left _ -> Left (BackendFailed SQLiteCommitFailed)
          Right result -> result


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


streamRows
  :: Connection
  -> EventOffset
  -> Conduit.ConduitT
       ()
       StoredEnvelope
       (ExceptT SQLiteError IO)
       ()
streamRows connection offset = do
  loaded <- liftIO (try @SQLError (loadEnvelopeRows connection offset))
  rows <- either (const (throwError SQLiteReadFailed)) pure loaded
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
      conflict <- firstConflict connection appends
      case conflict of
        Just found -> pure (Left found)
        Nothing -> do
          traverse_ (insertAppend connection) appends
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
        then pure (ValidatedAppend actual append)
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
