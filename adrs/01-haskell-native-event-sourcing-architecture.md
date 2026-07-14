# 01. Haskell-native event-sourcing architecture

- Status: Proposed
- Date: 2026-07-14
- Issue: https://github.com/dataclique/event-sorcery-hs/issues/1

## Context

`event-sorcery-hs` must provide the guarantees of the Rust `event-sorcery`
library in a form that is natural to Haskell rather than mechanically copying
Rust and `cqrs-es` implementation details. The important guarantees are:

- aggregate identifiers cannot be mixed across entity types;
- initialization and live-state transitions are different capabilities;
- command decisions and event folds are pure and fallible;
- events are immutable, ordered facts appended with optimistic concurrency;
- invalid or incompatible persisted data fails explicitly rather than producing
  partial state;
- projections can catch up and rebuild from the event log;
- schema drift invalidates derived state, never historical events;
- reactions and jobs are durable and at-least-once, with job resubmission fenced
  from an earlier ambiguous attempt; and
- an entity's dispatch intent and the corresponding job enqueue cannot commit
  separately.

The library needs only two complete backends: in-memory and SQLite. They must
obey the same behavioral contract and share a conformance suite. Supporting
every backend available to `cqrs-es` is explicitly out of scope.

The project is managed by Nix. `money-maker` is evidence of the desired
type-driven, functional style and contains an earlier `Eventful` experiment; it
is not a tooling template. `quanty` is the closer style reference for GHC2024,
Protolude, explicit imports, explicit deriving strategies, strict warnings,
Fourmolu, HLint, and a Nix development shell.

Haskell can express type constructors, associated data, and pure interpreters
directly. The Rust-only `Lifecycle` adapter, `ViewBackend` GAT emulation, and
`cqrs-es` vocabulary therefore do not belong in this public API.

## Decision

### Public domain contract

The public entry point is an `EventSourced entity` class with associated types
for `EntityId entity`, `Command entity`, `Event entity`, `CommandError entity`,
`ApplyError entity`, and a type-level list `Jobs entity`.

The class exposes stable aggregate, event-type, event-version, schema-version,
and identifier encodings. Its behavioral methods are pure:

```haskell
originate :: Event entity -> Either (ApplyError entity) entity
evolve :: entity -> Event entity -> Either (ApplyError entity) entity
initialize :: Command entity -> Either (CommandError entity) (Effect entity)
transition :: entity -> Command entity -> Either (CommandError entity) (Effect entity)
```

`initialize` has no entity argument, so initialization cannot inspect state that
does not exist. `transition` requires a live entity. `Effect entity` is a GADT
that contains either a non-empty batch of domain events or one typed job
dispatch. Dispatch requires type-level membership in `Jobs entity`; an
undeclared job is a compile error. There is no empty successful decision.

A dispatch carries one job value and cannot accompany arbitrary domain events.
Its constructor requires `Job job`, `Member job (Jobs entity)`, and
`Dispatches entity job`. The last capability injects the framework-owned
`DispatchIntent job` (a fresh `JobId` plus the serialized job intent) into
`Event entity`. The framework expands the dispatch into exactly two events:

- the injected intent event targets the origin entity stream and uses the
  stream head loaded for the command as its expected version; and
- `JobEnqueued` targets the framework-owned `("job", JobId)` stream and uses
  `NoStream`, because the framework generated that job id for this dispatch.

Before persistence, the intent event is applied to the origin entity and
`JobEnqueued` is applied to an empty framework job aggregate. A failure in
either fold rejects the decision. The two validated events then form one
two-stream `CommitBatch`; an expected-head mismatch or backend failure leaves
both streams unchanged. For a normal event decision, every event in the
non-empty batch is pre-applied to the origin entity in order and only that
origin stream is committed.

Replay is a total fold returning `Either (ReplayError entity) (Maybe entity)`.
The core pre-applies every decided event before persistence, so an invalid event
batch cannot poison a stream and fail only on its next load. Persisted decode,
metadata, ordering, and fold failures remain distinct constructors of
`ReplayError` and `StoreError`; errors are never flattened to text.

The public ownership boundary is:

| Failure | Owning error | Constructor and cause |
| --- | --- | --- |
| Event payload cannot decode or violates a decoded newtype invariant | `ReplayError entity` | `EventDecodeFailed StreamPosition DecodeCause`; wraps a sanitized decoder cause, never payload bytes |
| Aggregate type, aggregate id, event type, or event version differs from the requested stream contract | `ReplayError entity` | `EventMetadataMismatch StreamPosition MetadataMismatch`; wraps typed expected and actual metadata |
| Sequence is missing, duplicated, or out of order | `ReplayError entity` | `EventSequenceMismatch ExpectedSequence ActualSequence`; no backend exception |
| `originate` or `evolve` rejects an event | `ReplayError entity` | `EventApplicationFailed StreamPosition (ApplyError entity)`; preserves the typed domain cause |
| Loading a stream cannot replay it | `StoreError backend entity` | `ReplayFailed (ReplayError entity)`; the only nesting of `ReplayError` in `StoreError` |
| `initialize` or `transition` rejects a command | `StoreError backend entity` | `CommandRejected (CommandError entity)`; preserves the typed domain cause |
| An expected stream head changed | `StoreError backend entity` | `ConcurrencyConflict StreamKey ExpectedVersion ActualVersion`; an expected outcome, not a wrapped driver exception |
| Backend read, commit, checkpoint, or lease operation fails | `StoreError backend entity` | `BackendFailed (BackendError backend)`; preserves the typed backend cause while its renderer remains payload-redacted |
| A validated payload or batch limit is exceeded | `StoreError backend entity` | `CommitLimitExceeded CommitLimitViolation`; no backend write is attempted |

Consumers use `Store backend entity`, `Projection backend entity view`,
`Reactor backend entity`, and `JobRuntime backend`. Backend and entity types are
visible in signatures, while serialized envelopes, SQL, checkpoints, leases,
and internal lifecycle routing are not.

### Persistence contract

Backend classes operate on validated, backend-neutral serialized stream types.
They do not receive domain commands or entity values. `EventStore backend`
provides stream reads, snapshots, and an atomic compare-and-append of a
non-empty `CommitBatch` containing one or more expected stream heads.

Multi-stream commit is part of the primitive contract, not caller convention.
A normal command appends one stream. A job dispatch atomically appends the
origin's intent event and the job's enqueue event. Either every expected head
matches and the whole batch commits, or nothing commits.

`ExpectedVersion` is an ADT (`NoStream` or `At StreamVersion`), not an integer
sentinel or optional boolean. Stream positions, schema versions, event versions,
payload sizes, lease tokens, and attempt counts are validated newtypes. A
concurrency conflict is an expected result on the success/error boundary, not a
driver exception.

The in-memory backend stores the same serialized envelopes as SQLite in STM and
performs compare-and-append atomically. It deliberately exercises JSON decoding
and metadata validation instead of bypassing those paths with typed Haskell
values.

The SQLite backend uses `sqlite-simple` with explicit SQL and transactions. The
event key is `(aggregate_type, aggregate_id, sequence)`. A unique constraint is
the final concurrency arbiter, while the batch operation uses one write
transaction. Event rows are append-only; no public API updates or deletes them.
Snapshots, projection rows, checkpoints, schema registrations, job state, and
leases live in separate tables and may be rebuilt where their policy permits.

Projection and job storage are separate capability classes implemented by the
same two backend values. This keeps the small `Store` contract independent of
read models while allowing `JobRuntime backend` to require all capabilities.
The built-in wiring never combines an event store from one backend with job or
projection state from another.

### Derived state, reactions, and jobs

A `Projection` is a pure fold plus a durable checkpoint. Delivery is ordered per
stream and at-least-once across a crash boundary. Checkpoint advancement and the
corresponding view write are atomic. Reprocessing the current sequence is
absorbed; skipping a sequence is rejected. Projections support load, catch-up,
rebuild-one, and rebuild-all. SQLite-specific filtering is an optional
projection capability and does not leak SQL column names into the core fold.

Schema registration tracks stable aggregate and projection names with their
versions. A mismatch discards only snapshots and projection state that can be
replayed. Historical events are never rewritten. Retained streams may ignore an
incompatible snapshot and replay from events. A compacted stream fails closed
when its snapshot is incompatible because the snapshot may be its only complete
history.

Reactors consume committed envelopes through durable checkpoints. A reactor may
return a standalone-job dispatch or a typed command delivery, but neither is
executed inline. The runner atomically inserts a durable outbox entry and
advances the reactor checkpoint; a crash can therefore leave both pending or
neither recorded, never a checkpoint without its effect.

Every outbox entry has a stable `DeliveryId`. Workers retry transient delivery
failures and dead-letter terminal ones. Command delivery enters the target
`Store` through a dedicated delivery operation that atomically records the
`DeliveryId` receipt with any target events. Repeating an acknowledged or
ambiguous delivery observes the receipt and succeeds without handling the
command again. Thus a checkpoint restart may duplicate an outbox attempt but
cannot lose a command or produce its events twice. A reactor cannot emit an
untracked inline command.

Jobs use the same durable outbox, then continue as event-sourced state machines
with leases, fencing tokens, attempt counts, explicit transient/terminal
failure classification, retry, defer, and dead-letter outcomes. The first claim
may submit an external action; later claims must reconcile the earlier attempt
before any resubmission is authorized.

### Package and toolchain

The repository contains one Cabal library package named `event-sorcery` with a
small `EventSorcery` facade. Modules are grouped by domain capability, including
aggregate decisions, streams, projections, reactions, jobs, and each concrete
backend. There are no catch-all `Types`, `Errors`, `Utils`, or `Internal` public
modules.

`flake.nix` and `flake.lock` pin nixpkgs and expose the compiler, Cabal,
Haskell Language Server, Fourmolu, HLint, SQLite, and all library dependencies.
`nix develop` is the supported development environment and `nix flake check` is
the complete build, test, format, and lint gate. The package uses GHC2024,
Protolude with `NoImplicitPrelude`, explicit or qualified imports, explicit
deriving strategies, `-Wall`, and `-Werror`. Cabal is the package manifest;
Stack and Hpack do not form a second dependency-resolution layer.

Public behavior is pinned by tests before implementation. Observable contracts
include JSON encoding, stable aggregate/event names and versions, event order,
error constructors, conflict behavior, projection delivery order, and job
fencing. After the first release, public evolution is additive unless a major
version and a superseding ADR explicitly permit a break.

## Trust Boundaries and Required Abuse Tests

Persisted event/snapshot payloads and metadata cross from storage into typed
domain code and may be malformed through software defects or incompatible
schema evolution. Concurrent writers cross the optimistic-concurrency boundary.
Reactor checkpoints and job leases cross crash/restart and competing-worker
boundaries. User job code crosses from an external system back into durable
framework state. The SQLite database file, its directory, and the process that
opens it are inside the deployment's trusted boundary. The library validates
malformed or inconsistent stored data, but does not authenticate a valid row
replacement by an actor with file-write access. OS permissions, volume access,
and backup integrity protect that boundary.

The protected assets are the immutability and ordering of event history,
correct aggregate and projection state, atomic dispatch intent plus enqueue,
the authority to advance a job, and service availability. No secret payload or
metadata may be copied into logs or rendered errors.

The first behavioral tests after the public types compile must fail against
unimplemented behavior and then pass without changing their assertions:

- spoofed aggregate type, aggregate id, event type, or event version is rejected
  before a payload reaches the entity fold;
- malformed JSON, an invalid newtype, a sequence gap, duplicate, or reordering
  returns a typed replay failure without partial state;
- two commands racing on one stream yield one commit and one explicit conflict;
- failure during a two-stream dispatch commit leaves neither the intent nor the
  enqueue visible;
- a reactor crash cannot advance its checkpoint without its command outbox row,
  and retrying one `DeliveryId` cannot handle the target command twice;
- an oversized event is rejected before either backend stores it;
- projection restart may repeat the current envelope but cannot skip the next
  sequence or advance a checkpoint without its view update;
- a stale worker cannot acknowledge after its lease has been replaced;
- a later job attempt reconciles before resubmitting and an indeterminate result
  only defers;
- an incompatible retained snapshot replays from events, while an incompatible
  compacted snapshot fails closed; and
- decode and backend errors identify the stream position but never include the
  raw payload or metadata.

STRIDE consequences are handled as follows: identity and metadata spoofing are
checked against the typed stream key; malformed data, sequence tampering through
the library API, and concurrent writes are caught by decoding, ordering checks,
append-only operations, and compare-and-append, while authenticated detection
of a valid SQLite row replacement is explicitly not claimed; repudiation is
answered by the append-only history, delivery receipts, and job verdict events;
information disclosure is limited by redacted errors and no payload logging;
denial of service is bounded by validated payload and batch limits; and
elevation of privilege is prevented by keeping serialized commit constructors
behind `Store` and fencing every job state transition.

## Alternatives Considered

### Translate the Rust implementation and retain cqrs-es-shaped adapters

- Pros: Names and control flow would resemble the existing source line by line.
- Cons: Haskell has no `cqrs-es` engine to wrap, and Rust's lifecycle adapter and
  GAT workaround solve language-specific problems.
- Rejected because: it would preserve accidental Rust architecture while hiding
  the simpler Haskell domain model the new library exists to gain.

### Revive money-maker's MonadEventStore and open type-level error list

- Pros: It already demonstrates typed event names, typed ids, backend
  substitution, and explicit error membership in the user's preferred style.
- Cons: The monad-transformer surface, proxy plumbing, `Persistent` dependency,
  and monomorphic event-store interpreter do not encode optimistic concurrency,
  atomic multi-stream commits, projections, or fenced jobs.
- Rejected because: it is valuable prior art but extending it would make old
  application machinery the public architecture of a new general-purpose
  library.

### Model storage only as mtl or Effectful effects

- Pros: Interpreters make tests convenient and can keep application code
  polymorphic over effects.
- Cons: The public API acquires a large constraint vocabulary, transaction
  boundaries become implicit, and the backend/entity relationship becomes less
  visible at call sites.
- Rejected because: pure domain functions already provide most of the benefit;
  explicit `Store backend entity` values make atomicity and ownership clearer.
  Adapter effects can be supplied later without changing the core contract.

### Use persistent-sqlite instead of sqlite-simple

- Pros: `persistent` provides schema derivation, migrations, and typed entity
  access, and it was used by the earlier money-maker implementation.
- Cons: An append-only event log, expected-version compare-and-append, atomic
  multi-stream batches, and lease fencing still require explicit SQL and careful
  transaction control, while Template Haskell entities add a second domain
  model.
- Rejected because: `sqlite-simple` exposes the required transaction semantics
  directly with less abstraction and no generated persistence model.

### Split core, memory, and SQLite into separate packages immediately

- Pros: Core consumers would not acquire SQLite dependencies and each backend
  could version independently.
- Cons: Three packages multiply release, bounds, Nix, and documentation work
  before a public compatibility boundary is understood.
- Rejected because: the user requested exactly two built-in backends and one
  cohesive library. Modules preserve the boundary; packages can be extracted
  later if real consumers justify it.

### Use Stack and Hpack inside the Nix shell

- Pros: This matches `quanty` and gives a familiar snapshot workflow.
- Cons: It introduces a second package/toolchain resolution path beside the
  pinned Nix flake and generates the Cabal manifest from another source file.
- Rejected because: the repository should have one authoritative Nix toolchain
  and one authoritative Cabal package description.

## Consequences

The public surface is smaller than the Rust surface because Haskell does not
need its adapter types, but the behavioral scope remains the same. Domain code
is pure and backend-independent. In-memory tests exercise the same wire format
and conflict semantics as SQLite, so a green fast suite is meaningful rather
than a mock-only signal.

Atomic multi-stream commit makes both backend implementations more demanding,
but it makes the dispatch invariant impossible for callers to forget. Durable
projections and jobs add checkpoint, lease, and conformance machinery; those
costs are accepted because crash recovery and ambiguous external submissions
are part of the library's value, not application details.

JSON encodings and stable names become long-lived contracts. Changing a shipped
event shape requires a new event version and an explicit upcaster; changing an
aggregate or projection shape requires a schema-version bump. Compacting events
is opt-in and permanently narrows recovery options.

The single package depends on SQLite even for consumers that only use memory.
If that becomes a demonstrated distribution problem, backend package extraction
will require a new ADR but not a change to the domain classes.

All development and verification require Nix. Editors and ad-hoc host toolchains
are not supported sources of truth.
