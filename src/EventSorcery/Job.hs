module EventSorcery.Job (
  AttemptCount (..),
  JobClaim (..),
  JobEnqueue (..),
  JobError (..),
  JobId,
  JobStore (..),
  LeaseInstant (..),
  LeaseToken (..),
  LeaseWindow,
  mkLeaseWindow,
) where

import EventSorcery.Job.Internal
import Protolude


mkLeaseWindow :: LeaseInstant -> LeaseInstant -> Maybe LeaseWindow
mkLeaseWindow claimedAt expiresAt
  | expiresAt <= claimedAt = Nothing
  | otherwise = Just (LeaseWindow claimedAt expiresAt)
