module EventSorcery.Aggregate (
  DecodeCause (..),
  DispatchIntent,
  Dispatches (..),
  Effect (..),
  EventSourced (..),
  EventVersion (..),
  Job (..),
  JobId,
  Member,
  SchemaVersion (..),
  dispatchIntent,
  dispatchJobId,
  jobIdText,
  mkJobId,
) where

import Data.Kind (Type)
import Data.Type.Equality (type (~))
import Protolude hiding (Type)


newtype EventVersion = EventVersion Word16
  deriving stock (Eq, Ord, Show)


newtype SchemaVersion = SchemaVersion Word16
  deriving stock (Eq, Ord, Show)


newtype DecodeCause = DecodeCause Text
  deriving stock (Eq, Show)


newtype JobId = JobId Text
  deriving stock (Eq, Ord, Show)


data DispatchIntent job = DispatchIntent JobId Text ByteString


class Job job where
  jobType :: Proxy job -> Text
  encodeJob :: job -> ByteString
  decodeJob :: ByteString -> Either DecodeCause job


class Dispatches entity job where
  injectDispatchIntent :: DispatchIntent job -> Event entity


type Member item items = Elem item items ~ 'True


type family Elem (item :: Type) (items :: [Type]) :: Bool where
  Elem item '[] = 'False
  Elem item (item ': items) = 'True
  Elem item (other ': items) = Elem item items


data Effect entity where
  Events :: NonEmpty (Event entity) -> Effect entity
  Dispatch
    :: (Job job, Member job (Jobs entity), Dispatches entity job)
    => job
    -> Effect entity


class EventSourced entity where
  type EntityId entity = (identifier :: Type) | identifier -> entity
  type Command entity :: Type
  type Event entity = (event :: Type) | event -> entity
  type CommandError entity :: Type
  type ApplyError entity :: Type
  type Jobs entity :: [Type]


  aggregateType :: Proxy entity -> Text
  encodeEntityId :: EntityId entity -> Text
  eventType :: Event entity -> Text
  eventVersion :: Event entity -> EventVersion
  schemaVersion :: Proxy entity -> SchemaVersion
  encodeEvent :: Event entity -> ByteString
  decodeEvent :: ByteString -> Either DecodeCause (Event entity)
  encodeSnapshot :: entity -> ByteString
  decodeSnapshot :: ByteString -> Either DecodeCause entity
  originate :: Event entity -> Either (ApplyError entity) entity
  evolve :: entity -> Event entity -> Either (ApplyError entity) entity
  initialize :: Command entity -> Either (CommandError entity) (Effect entity)
  transition
    :: entity -> Command entity -> Either (CommandError entity) (Effect entity)


mkJobId :: Text -> Maybe JobId
mkJobId value
  | value == "" = Nothing
  | otherwise = Just (JobId value)


jobIdText :: JobId -> Text
jobIdText (JobId value) = value


dispatchIntent :: forall job. Job job => JobId -> job -> DispatchIntent job
dispatchIntent identifier job =
  DispatchIntent identifier (jobType (Proxy @job)) (encodeJob job)


dispatchJobId :: DispatchIntent job -> JobId
dispatchJobId (DispatchIntent identifier _ _) = identifier
