module EventSorcery.Job (
  AttemptCount (..),
  AttemptLimit,
  DeadReason (..),
  DurableJob (..),
  JobClaim (..),
  JobContext,
  JobEnqueue (..),
  JobError (..),
  JobFailure (..),
  JobId,
  JobOutcome (..),
  JobRunError (..),
  JobRunResult (..),
  JobRuntime,
  JobStore (..),
  LeaseInstant (..),
  LeaseToken (..),
  LeaseWindow,
  Reconciliation (..),
  enqueueDurableJob,
  jobContextAttempt,
  jobContextId,
  mkAttemptLimit,
  mkJobRuntime,
  mkLeaseWindow,
  runJobOnce,
) where

import EventSorcery.Aggregate (DecodeCause, Job)
import EventSorcery.Job.Internal
import Protolude


mkLeaseWindow :: LeaseInstant -> LeaseInstant -> Maybe LeaseWindow
mkLeaseWindow claimedAt expiresAt
  | expiresAt <= claimedAt = Nothing
  | otherwise = Just (LeaseWindow claimedAt expiresAt)


mkAttemptLimit :: Word64 -> Maybe AttemptLimit
mkAttemptLimit attempts
  | attempts == 0 = Nothing
  | otherwise = Just (AttemptLimit attempts)


mkJobRuntime
  :: backend
  -> AttemptLimit
  -> (AttemptCount -> LeaseInstant)
  -> JobRuntime backend
mkJobRuntime = JobRuntime


jobContextId :: JobContext -> JobId
jobContextId (JobContext identifier _) = identifier


jobContextAttempt :: JobContext -> AttemptCount
jobContextAttempt (JobContext _ attempt) = attempt


enqueueDurableJob
  :: (Job job, JobStore backend)
  => JobRuntime backend
  -> JobId
  -> job
  -> IO (Either (JobError backend) JobEnqueue)
enqueueDurableJob (JobRuntime backend _ _) identifier job =
  enqueueJob backend identifier (encodeStoredJob job)


runJobOnce
  :: forall job backend
   . (DurableJob job, JobStore backend)
  => Proxy job
  -> JobRuntime backend
  -> JobInput job
  -> JobId
  -> LeaseWindow
  -> IO
       ( Either
           (JobRunError backend)
           (JobRunResult (JobOutput job) (JobFailureCause job))
       )
runJobOnce _ runtime@(JobRuntime backend _ _) input identifier window = do
  claimed <- claimJob backend identifier window
  case claimed of
    Left failure -> pure (Left (JobRunStoreFailed failure))
    Right (JobClaim token attempts payload) ->
      case decodeStoredJob @job payload of
        Left failure -> rejectUndecodable backend identifier token failure
        Right job -> do
          executed <-
            executeDurableJob
              (executionRoute token)
              (JobContext identifier attempts)
              input
              job
          persistJobExecution runtime identifier token attempts executed


data JobExecutionRoute
  = SubmitExecution
  | ReconcileExecution


data RetryDisposition
  = ScheduleRetry AttemptCount
  | ExhaustRetries AttemptCount


executionRoute :: LeaseToken -> JobExecutionRoute
executionRoute (LeaseToken 1) = SubmitExecution
executionRoute _ = ReconcileExecution


executeDurableJob
  :: DurableJob job
  => JobExecutionRoute
  -> JobContext
  -> JobInput job
  -> job
  -> IO
       ( Either
           (JobFailure (JobFailureCause job))
           (JobOutcome (JobOutput job))
       )
executeDurableJob route context input job = case route of
  SubmitExecution -> submitJob context input job
  ReconcileExecution -> do
    reconciled <- reconcileJob context input job
    case reconciled of
      Left failure -> pure (Left failure)
      Right (Settled output) -> pure (Right (JobDone output))
      Right NotSubmitted -> submitJob context input job
      Right (Indeterminate runAt) -> pure (Right (JobDefer runAt))


persistJobExecution
  :: JobStore backend
  => JobRuntime backend
  -> JobId
  -> LeaseToken
  -> AttemptCount
  -> Either (JobFailure failure) (JobOutcome output)
  -> IO (Either (JobRunError backend) (JobRunResult output failure))
persistJobExecution runtime@(JobRuntime backend _ _) identifier token attempts =
  \case
    Right (JobDone output) -> do
      acknowledged <- acknowledgeJob backend identifier token
      pure (JobSucceeded output <$ first JobRunStoreFailed acknowledged)
    Right (JobDefer runAt) -> do
      deferred <- deferJob backend identifier token runAt
      pure (JobDeferred runAt <$ first JobRunStoreFailed deferred)
    Left (Terminal failure) -> do
      rejected <- deadLetterJob backend identifier token Rejected
      pure (JobRejected failure <$ first JobRunStoreFailed rejected)
    Left (Transient failure) ->
      persistTransientFailure runtime identifier token attempts failure


persistTransientFailure
  :: JobStore backend
  => JobRuntime backend
  -> JobId
  -> LeaseToken
  -> AttemptCount
  -> failure
  -> IO (Either (JobRunError backend) (JobRunResult output failure))
persistTransientFailure
  (JobRuntime backend limit retryAt)
  identifier
  token
  attempts
  failure = case retryDisposition limit attempts of
    Nothing -> do
      exhausted <- exhaustJob backend identifier token
      pure case first JobRunStoreFailed exhausted of
        Left storeFailure -> Left storeFailure
        Right actual ->
          Left (JobRunAttemptMismatch attempts actual)
    Just (ScheduleRetry expected) -> do
      let runAt = retryAt expected
      retried <- retryJob backend identifier token runAt
      pure case first JobRunStoreFailed retried of
        Left storeFailure -> Left storeFailure
        Right actual
          | actual == expected ->
              Right (JobRetryScheduled actual runAt failure)
          | otherwise -> Left (JobRunAttemptMismatch expected actual)
    Just (ExhaustRetries expected) -> do
      exhausted <- exhaustJob backend identifier token
      pure case first JobRunStoreFailed exhausted of
        Left storeFailure -> Left storeFailure
        Right actual
          | actual == expected ->
              Right (JobRetriesExhausted actual failure)
          | otherwise -> Left (JobRunAttemptMismatch expected actual)


retryDisposition
  :: AttemptLimit -> AttemptCount -> Maybe RetryDisposition
retryDisposition
  (AttemptLimit limit)
  (AttemptCount attempts)
    | attempts == maxBound = Nothing
    | next >= limit = Just (ExhaustRetries (AttemptCount next))
    | otherwise = Just (ScheduleRetry (AttemptCount next))
    where
      next = attempts + 1


rejectUndecodable
  :: JobStore backend
  => backend
  -> JobId
  -> LeaseToken
  -> DecodeCause
  -> IO (Either (JobRunError backend) result)
rejectUndecodable backend identifier token failure = do
  rejected <- deadLetterJob backend identifier token Undecodable
  pure case first JobRunStoreFailed rejected of
    Left storeFailure -> Left storeFailure
    Right () -> Left (JobRunDecodeFailed identifier failure)
