# event-sorcery-hs

Event sourcing with type-level magic, now in Haskell

The Haskell counterpart to
[`event-sorcery`](https://github.com/dataclique/event-sorcery): pure,
type-driven event-sourcing primitives with in-memory and SQLite persistence.

Its architecture is recorded in the
[accepted architecture ADR](adrs/01-haskell-native-event-sourcing-architecture.md).

The library provides:

- pure, fallible aggregate decisions with typed identifiers and job dispatches;
- atomic multi-stream commits with optimistic concurrency in memory and SQLite;
- Conduit-based projection and reactor runners with durable checkpoints;
- schema-aware snapshots and derived-state reconciliation;
- idempotent typed command delivery through durable delivery receipts;
- fenced, retryable reactor outboxes with retained dead letters; and
- event-sourced durable jobs with leases, reconciliation, retries, and explicit
  terminal outcomes.

The two backends share one conformance suite, so the same ordering, atomicity,
replay, delivery, projection, reactor, schema, snapshot, and job contracts run
against both implementations.

## Development

The project is managed entirely through Nix. With nix-direnv:

```console
direnv allow
stack test
```

Alternatively, enter the development shell manually:

```console
nix develop
stack test
```

Run the complete CI-equivalent gate from either environment:

```console
nix flake check
```

Run the Criterion performance suite from either environment:

```sh
stack bench
stack bench --benchmark-arguments "--regress allocated:iters +RTS -T -RTS"
```

The suite forces replay inputs before measurement and isolates mutable write
fixtures per run. It covers replay, Conduit catch-up, projections, snapshots,
typed command execution, durable jobs, reactor outboxes, and SQLite commits so
time and allocation regressions expose thunk buildup and GC pressure.

Direnv loads the flake automatically. The development shell provides GHC 9.14.1,
Stack, Cabal, Fourmolu, HLint, and SQLite. Stack uses the Nix-provided compiler
rather than installing its own. `nix flake check` builds the library and runs
every formatting, lint, and test gate.
