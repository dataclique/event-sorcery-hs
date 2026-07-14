module EventSorcery.Backend.SQLite (
  SQLiteError (..),
  SQLiteFailure (..),
  SQLiteStore,
  closeSQLiteStore,
  openSQLiteStore,
) where

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


instance FromRow EventRow where
  fromRow = EventRow <$> field <*> field <*> field <*> field


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


  commit = commitSQLite


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
migrate connection =
  execute_
    connection
    """
    CREATE TABLE IF NOT EXISTS events (
      aggregate_type TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      event_version INTEGER NOT NULL,
      payload BLOB NOT NULL,
      PRIMARY KEY (aggregate_type, aggregate_id, sequence)
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
