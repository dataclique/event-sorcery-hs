module EventSorcery.Job.Internal (
  AttemptCount (..),
  AttemptLimit (..),
  DeadReason (..),
  DurableJob (..),
  JobClaim (..),
  JobContext (..),
  JobEnqueue (..),
  JobError (..),
  JobFailure (..),
  JobId,
  JobLifecycleEvent (..),
  JobOutcome (..),
  JobRecord (..),
  JobRunError (..),
  JobRunResult (..),
  JobRuntime (..),
  JobStatus (..),
  JobStore (..),
  LeaseInstant (..),
  LeaseToken (..),
  LeaseWindow (..),
  Reconciliation (..),
  decideAcknowledge,
  decideClaim,
  decideDeadLetter,
  decideDefer,
  decideEnqueue,
  decideExhaust,
  decideRetry,
  decodeStoredJob,
  encodeJobRecord,
  encodeStoredJob,
  frameworkJobSeed,
  jobEventAppend,
) where

import Control.Monad.Fail qualified as MonadFail
import Data.Binary.Get qualified as Binary
import Data.Binary.Put qualified as Binary
import Data.ByteString qualified as ByteString
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LazyByteString
import Data.List.NonEmpty qualified as NonEmpty
import EventSorcery.Aggregate
import EventSorcery.Store.Internal
import EventSorcery.Stream (EventMetadata (..), ExpectedVersion)
import Protolude


newtype LeaseInstant = LeaseInstant Word64
  deriving stock (Eq, Ord, Show)


newtype LeaseToken = LeaseToken Word64
  deriving stock (Eq, Ord, Show)


newtype AttemptCount = AttemptCount Word64
  deriving stock (Eq, Ord, Show)


newtype AttemptLimit = AttemptLimit Word64
  deriving stock (Eq, Ord, Show)


data LeaseWindow = LeaseWindow LeaseInstant LeaseInstant
  deriving stock (Eq, Show)


instance NFData LeaseWindow where
  rnf (LeaseWindow claimedAt expiresAt) =
    claimedAt `seq` expiresAt `seq` ()


data DeadReason
  = RetriesExhausted
  | Rejected
  | Undecodable
  | Abandoned
  deriving stock (Eq, Show)


data JobClaim = JobClaim LeaseToken AttemptCount ByteString
  deriving stock (Eq, Show)


data JobEnqueue
  = JobEnqueued
  | JobAlreadyEnqueued
  deriving stock (Eq, Show)


data JobError backend
  = JobNotFound JobId
  | JobPayloadMismatch JobId
  | JobNotRunnable JobId LeaseInstant
  | JobLeaseUnavailable JobId LeaseInstant
  | JobNotLeased JobId
  | JobLeaseLost JobId LeaseToken LeaseToken
  | JobAlreadyCompleted JobId
  | JobAlreadyDeadLettered JobId DeadReason
  | JobLeaseTokenExhausted JobId
  | JobAttemptCountExhausted JobId
  | JobBackendFailed (BackendError backend)


deriving stock instance Eq (BackendError backend) => Eq (JobError backend)


deriving stock instance Show (BackendError backend) => Show (JobError backend)


data JobContext = JobContext JobId AttemptCount


data JobFailure failure
  = Transient failure
  | Terminal failure
  deriving stock (Eq, Show)


data JobOutcome output
  = JobDone output
  | JobDefer LeaseInstant
  deriving stock (Eq, Show)


data Reconciliation output
  = Settled output
  | NotSubmitted
  | Indeterminate LeaseInstant
  deriving stock (Eq, Show)


class Job job => DurableJob job where
  type JobInput job
  type JobOutput job
  type JobFailureCause job


  submitJob
    :: JobContext
    -> JobInput job
    -> job
    -> IO
         ( Either
             (JobFailure (JobFailureCause job))
             (JobOutcome (JobOutput job))
         )
  reconcileJob
    :: JobContext
    -> JobInput job
    -> job
    -> IO
         ( Either
             (JobFailure (JobFailureCause job))
             (Reconciliation (JobOutput job))
         )


data JobRuntime backend
  = JobRuntime backend AttemptLimit (AttemptCount -> LeaseInstant)


data JobRunError backend
  = JobRunStoreFailed (JobError backend)
  | JobRunDecodeFailed JobId DecodeCause
  | JobRunAttemptMismatch AttemptCount AttemptCount


deriving stock instance Eq (BackendError backend) => Eq (JobRunError backend)


deriving stock instance
  Show (BackendError backend) => Show (JobRunError backend)


data JobRunResult output failure
  = JobSucceeded output
  | JobDeferred LeaseInstant
  | JobRetryScheduled AttemptCount LeaseInstant failure
  | JobRejected failure
  | JobRetriesExhausted AttemptCount failure
  deriving stock (Eq, Show)


class EventStore backend => JobStore backend where
  enqueueJob
    :: backend
    -> JobId
    -> ByteString
    -> IO (Either (JobError backend) JobEnqueue)
  claimJob
    :: backend
    -> JobId
    -> LeaseWindow
    -> IO (Either (JobError backend) JobClaim)
  acknowledgeJob
    :: backend
    -> JobId
    -> LeaseToken
    -> IO (Either (JobError backend) ())
  retryJob
    :: backend
    -> JobId
    -> LeaseToken
    -> LeaseInstant
    -> IO (Either (JobError backend) AttemptCount)
  deferJob
    :: backend
    -> JobId
    -> LeaseToken
    -> LeaseInstant
    -> IO (Either (JobError backend) ())
  deadLetterJob
    :: backend
    -> JobId
    -> LeaseToken
    -> DeadReason
    -> IO (Either (JobError backend) ())
  exhaustJob
    :: backend
    -> JobId
    -> LeaseToken
    -> IO (Either (JobError backend) AttemptCount)


data JobRecord = JobRecord ByteString JobStatus LeaseToken AttemptCount
  deriving stock (Eq, Show)


data JobStatus
  = JobReady
  | JobScheduled LeaseInstant
  | JobLeased LeaseInstant
  | JobCompleted
  | JobDeadLettered DeadReason
  deriving stock (Eq, Show)


data JobLifecycleEvent
  = JobEnqueuedEvent
  | JobClaimedEvent
  | JobSucceededEvent
  | JobRetryScheduledEvent
  | JobDeferredEvent
  | JobDeadLetteredEvent
  deriving stock (Eq, Show)


encodeStoredJob :: forall job. Job job => job -> ByteString
encodeStoredJob job =
  LazyByteString.toStrict
    ( Builder.toLazyByteString
        ( Builder.word64BE (fromIntegral (ByteString.length jobTypeBytes))
            <> Builder.byteString jobTypeBytes
            <> Builder.byteString (encodeJob job)
        )
    )
  where
    jobTypeBytes = encodeUtf8 (jobType (Proxy @job))


decodeStoredJob :: forall job. Job job => ByteString -> Either DecodeCause job
decodeStoredJob stored = do
  typeLength <- decodeTypeLength stored
  let afterHeader = ByteString.drop storedJobHeaderSize stored
      encodedType = ByteString.take typeLength afterHeader
      payload = ByteString.drop typeLength afterHeader
      expectedType = encodeUtf8 (jobType (Proxy @job))
  if ByteString.length encodedType /= typeLength
    then Left invalidStoredJob
    else
      if encodedType /= expectedType
        then Left (DecodeCause "stored job type mismatch")
        else decodeJob payload


decodeTypeLength :: ByteString -> Either DecodeCause Int
decodeTypeLength stored
  | ByteString.length stored < storedJobHeaderSize = Left invalidStoredJob
  | encoded > fromIntegral (maxBound :: Int) = Left invalidStoredJob
  | otherwise = Right (fromIntegral encoded)
  where
    encoded =
      foldl'
        (\value byte -> value * 256 + fromIntegral byte)
        (0 :: Word64)
        (ByteString.unpack (ByteString.take storedJobHeaderSize stored))


storedJobHeaderSize :: Int
storedJobHeaderSize = 8


invalidStoredJob :: DecodeCause
invalidStoredJob = DecodeCause "invalid stored job"


encodeJobRecord :: JobRecord -> ByteString
encodeJobRecord record =
  LazyByteString.toStrict (Binary.runPut (putJobRecord record))


decodeJobRecord :: ByteString -> Either DecodeCause JobRecord
decodeJobRecord encoded =
  case Binary.runGetOrFail getJobRecord (LazyByteString.fromStrict encoded) of
    Left _ -> Left invalidStoredJobState
    Right (remaining, _, record)
      | LazyByteString.null remaining -> Right record
      | otherwise -> Left invalidStoredJobState


frameworkJobSeed
  :: StreamAppend
  -> Maybe (JobId, Either DecodeCause JobRecord)
frameworkJobSeed append = do
  identifier <- case streamAppendIdentity append of
    StreamIdentity "job" encoded -> mkJobId encoded
    _ -> Nothing
  let ProposedEvent metadata payload =
        NonEmpty.head (streamAppendEvents append)
  guard (metadata.eventType == "enqueued")
  guard (metadata.eventVersion == EventVersion 1)
  pure (identifier, decodeJobRecord payload)


jobEventAppend
  :: JobId
  -> ExpectedVersion
  -> JobLifecycleEvent
  -> JobRecord
  -> StreamAppend
jobEventAppend identifier expected lifecycle record =
  StreamAppend
    (StreamIdentity "job" encodedId)
    expected
    ( ProposedEvent
        ( EventMetadata
            "job"
            encodedId
            (jobLifecycleEventType lifecycle)
            (EventVersion 1)
        )
        (encodeJobRecord record)
        :| []
    )
  where
    encodedId = jobIdText identifier


putJobRecord :: JobRecord -> Binary.Put
putJobRecord (JobRecord payload status (LeaseToken token) (AttemptCount attempts)) = do
  Binary.putWord8 jobRecordEncodingVersion
  Binary.putWord64be (fromIntegral (ByteString.length payload))
  Binary.putByteString payload
  putJobStatus status
  Binary.putWord64be token
  Binary.putWord64be attempts


getJobRecord :: Binary.Get JobRecord
getJobRecord = do
  version <- Binary.getWord8
  guard (version == jobRecordEncodingVersion)
  payloadLength <- Binary.getWord64be
  guard (payloadLength <= fromIntegral (maxBound :: Int))
  payload <- Binary.getByteString (fromIntegral payloadLength)
  status <- getJobStatus
  token <- LeaseToken <$> Binary.getWord64be
  attempts <- AttemptCount <$> Binary.getWord64be
  pure (JobRecord payload status token attempts)


putJobStatus :: JobStatus -> Binary.Put
putJobStatus status = case status of
  JobReady -> Binary.putWord8 0
  JobScheduled (LeaseInstant runAt) -> do
    Binary.putWord8 1
    Binary.putWord64be runAt
  JobLeased (LeaseInstant expiresAt) -> do
    Binary.putWord8 2
    Binary.putWord64be expiresAt
  JobCompleted -> Binary.putWord8 3
  JobDeadLettered reason -> do
    Binary.putWord8 4
    putDeadReason reason


getJobStatus :: Binary.Get JobStatus
getJobStatus = do
  tag <- Binary.getWord8
  case tag of
    0 -> pure JobReady
    1 -> JobScheduled . LeaseInstant <$> Binary.getWord64be
    2 -> JobLeased . LeaseInstant <$> Binary.getWord64be
    3 -> pure JobCompleted
    4 -> JobDeadLettered <$> getDeadReason
    _ -> MonadFail.fail "invalid job status"


putDeadReason :: DeadReason -> Binary.Put
putDeadReason reason = Binary.putWord8 case reason of
  RetriesExhausted -> 0
  Rejected -> 1
  Undecodable -> 2
  Abandoned -> 3


getDeadReason :: Binary.Get DeadReason
getDeadReason = do
  tag <- Binary.getWord8
  case tag of
    0 -> pure RetriesExhausted
    1 -> pure Rejected
    2 -> pure Undecodable
    3 -> pure Abandoned
    _ -> MonadFail.fail "invalid dead-letter reason"


jobLifecycleEventType :: JobLifecycleEvent -> Text
jobLifecycleEventType lifecycle = case lifecycle of
  JobEnqueuedEvent -> "enqueued"
  JobClaimedEvent -> "claimed"
  JobSucceededEvent -> "succeeded"
  JobRetryScheduledEvent -> "retry-scheduled"
  JobDeferredEvent -> "deferred"
  JobDeadLetteredEvent -> "dead-lettered"


jobRecordEncodingVersion :: Word8
jobRecordEncodingVersion = 1


invalidStoredJobState :: DecodeCause
invalidStoredJobState = DecodeCause "invalid stored job state"


decideEnqueue
  :: JobId
  -> ByteString
  -> Maybe JobRecord
  -> Either (JobError backend) (JobEnqueue, JobRecord)
decideEnqueue _ payload Nothing =
  Right
    ( JobEnqueued
    , JobRecord payload JobReady (LeaseToken 0) (AttemptCount 0)
    )
decideEnqueue identifier payload (Just current@(JobRecord stored _ _ _))
  | payload == stored = Right (JobAlreadyEnqueued, current)
  | otherwise = Left (JobPayloadMismatch identifier)


decideClaim
  :: JobId
  -> LeaseWindow
  -> Maybe JobRecord
  -> Either (JobError backend) (JobClaim, JobRecord)
decideClaim identifier _ Nothing = Left (JobNotFound identifier)
decideClaim identifier window@(LeaseWindow claimedAt _) (Just record) =
  case record of
    JobRecord _ JobCompleted _ _ -> Left (JobAlreadyCompleted identifier)
    JobRecord _ (JobDeadLettered reason) _ _ ->
      Left (JobAlreadyDeadLettered identifier reason)
    JobRecord _ (JobScheduled runAt) _ _
      | claimedAt < runAt -> Left (JobNotRunnable identifier runAt)
    JobRecord _ (JobLeased expiresAt) _ _
      | claimedAt < expiresAt ->
          Left (JobLeaseUnavailable identifier expiresAt)
    _ -> claimAvailable identifier window record


decideAcknowledge
  :: JobId
  -> LeaseToken
  -> Maybe JobRecord
  -> Either (JobError backend) JobRecord
decideAcknowledge identifier _ Nothing = Left (JobNotFound identifier)
decideAcknowledge
  identifier
  requested
  (Just record@(JobRecord payload status current attempts))
    | requested /= current = Left (JobLeaseLost identifier requested current)
    | otherwise = case status of
        JobReady -> Left (JobNotLeased identifier)
        JobScheduled _ -> Left (JobNotLeased identifier)
        JobLeased _ -> Right (JobRecord payload JobCompleted current attempts)
        JobCompleted -> Right record
        JobDeadLettered reason -> Left (JobAlreadyDeadLettered identifier reason)


decideRetry
  :: JobId
  -> LeaseToken
  -> LeaseInstant
  -> Maybe JobRecord
  -> Either (JobError backend) (AttemptCount, JobRecord)
decideRetry identifier token runAt current = do
  (payload, held, attempts) <- leasedJob identifier token current
  nextAttempt <- incrementAttemptCount identifier attempts
  pure
    ( nextAttempt
    , JobRecord payload (JobScheduled runAt) held nextAttempt
    )


decideDefer
  :: JobId
  -> LeaseToken
  -> LeaseInstant
  -> Maybe JobRecord
  -> Either (JobError backend) JobRecord
decideDefer identifier token runAt current = do
  (payload, held, attempts) <- leasedJob identifier token current
  pure (JobRecord payload (JobScheduled runAt) held attempts)


decideDeadLetter
  :: JobId
  -> LeaseToken
  -> DeadReason
  -> Maybe JobRecord
  -> Either (JobError backend) JobRecord
decideDeadLetter identifier requested reason current = case current of
  Just record@(JobRecord _ (JobDeadLettered storedReason) held _)
    | requested == held && reason == storedReason -> Right record
  _ -> do
    (payload, held, attempts) <- leasedJob identifier requested current
    pure (JobRecord payload (JobDeadLettered reason) held attempts)


decideExhaust
  :: JobId
  -> LeaseToken
  -> Maybe JobRecord
  -> Either (JobError backend) (AttemptCount, JobRecord)
decideExhaust identifier token current = do
  (payload, held, attempts) <- leasedJob identifier token current
  nextAttempt <- incrementAttemptCount identifier attempts
  pure
    ( nextAttempt
    , JobRecord
        payload
        (JobDeadLettered RetriesExhausted)
        held
        nextAttempt
    )


claimAvailable
  :: JobId
  -> LeaseWindow
  -> JobRecord
  -> Either (JobError backend) (JobClaim, JobRecord)
claimAvailable
  identifier
  (LeaseWindow _ expiresAt)
  (JobRecord payload _ token attempts) = do
    nextToken <- incrementLeaseToken identifier token
    pure
      ( JobClaim nextToken attempts payload
      , JobRecord payload (JobLeased expiresAt) nextToken attempts
      )


leasedJob
  :: JobId
  -> LeaseToken
  -> Maybe JobRecord
  -> Either
       (JobError backend)
       (ByteString, LeaseToken, AttemptCount)
leasedJob identifier _ Nothing = Left (JobNotFound identifier)
leasedJob
  identifier
  requested
  (Just (JobRecord payload status held attempts))
    | requested /= held = Left (JobLeaseLost identifier requested held)
    | otherwise = case status of
        JobReady -> Left (JobNotLeased identifier)
        JobScheduled _ -> Left (JobNotLeased identifier)
        JobLeased _ -> Right (payload, held, attempts)
        JobCompleted -> Left (JobAlreadyCompleted identifier)
        JobDeadLettered reason ->
          Left (JobAlreadyDeadLettered identifier reason)


incrementLeaseToken
  :: JobId -> LeaseToken -> Either (JobError backend) LeaseToken
incrementLeaseToken identifier (LeaseToken token)
  | token == maxBound = Left (JobLeaseTokenExhausted identifier)
  | otherwise = Right (LeaseToken (token + 1))


incrementAttemptCount
  :: JobId -> AttemptCount -> Either (JobError backend) AttemptCount
incrementAttemptCount identifier (AttemptCount attempts)
  | attempts == maxBound = Left (JobAttemptCountExhausted identifier)
  | otherwise = Right (AttemptCount (attempts + 1))
