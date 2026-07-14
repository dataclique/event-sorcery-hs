module EventSorcery.Job.Internal (
  AttemptCount (..),
  JobClaim (..),
  JobEnqueue (..),
  JobError (..),
  JobId,
  JobRecord (..),
  JobStatus (..),
  JobStore (..),
  LeaseInstant (..),
  LeaseToken (..),
  LeaseWindow (..),
  decideAcknowledge,
  decideClaim,
  decideEnqueue,
) where

import EventSorcery.Aggregate (JobId)
import EventSorcery.Store.Internal
import Protolude


newtype LeaseInstant = LeaseInstant Word64
  deriving stock (Eq, Ord, Show)


newtype LeaseToken = LeaseToken Word64
  deriving stock (Eq, Ord, Show)


newtype AttemptCount = AttemptCount Word64
  deriving stock (Eq, Ord, Show)


data LeaseWindow = LeaseWindow LeaseInstant LeaseInstant
  deriving stock (Eq, Show)


instance NFData LeaseWindow where
  rnf (LeaseWindow claimedAt expiresAt) =
    claimedAt `seq` expiresAt `seq` ()


data JobClaim = JobClaim LeaseToken AttemptCount ByteString
  deriving stock (Eq, Show)


data JobEnqueue
  = JobEnqueued
  | JobAlreadyEnqueued
  deriving stock (Eq, Show)


data JobError backend
  = JobNotFound JobId
  | JobPayloadMismatch JobId
  | JobLeaseUnavailable JobId LeaseInstant
  | JobNotLeased JobId
  | JobLeaseLost JobId LeaseToken LeaseToken
  | JobAlreadyCompleted JobId
  | JobLeaseTokenExhausted JobId
  | JobAttemptCountExhausted JobId
  | JobBackendFailed (BackendError backend)


deriving stock instance Eq (BackendError backend) => Eq (JobError backend)


deriving stock instance Show (BackendError backend) => Show (JobError backend)


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


data JobRecord = JobRecord ByteString JobStatus LeaseToken AttemptCount
  deriving stock (Eq, Show)


data JobStatus
  = JobReady
  | JobLeased LeaseInstant
  | JobCompleted
  deriving stock (Eq, Show)


decideEnqueue
  :: JobId
  -> ByteString
  -> Maybe JobRecord
  -> Either (JobError backend) (JobEnqueue, Maybe JobRecord)
decideEnqueue _ payload Nothing =
  Right
    ( JobEnqueued
    , Just (JobRecord payload JobReady (LeaseToken 0) (AttemptCount 0))
    )
decideEnqueue identifier payload current@(Just (JobRecord stored _ _ _))
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
decideAcknowledge identifier requested (Just record@(JobRecord payload status current attempts))
  | requested /= current = Left (JobLeaseLost identifier requested current)
  | otherwise = case status of
      JobReady -> Left (JobNotLeased identifier)
      JobLeased _ -> Right (JobRecord payload JobCompleted current attempts)
      JobCompleted -> Right record


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
    nextAttempt <- incrementAttemptCount identifier attempts
    pure
      ( JobClaim nextToken nextAttempt payload
      , JobRecord payload (JobLeased expiresAt) nextToken nextAttempt
      )


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
