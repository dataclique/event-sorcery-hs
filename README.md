# event-sorcery-hs

Event sourcing with type-level magic, now in Haskell

The Haskell counterpart to
[`event-sorcery`](https://github.com/dataclique/event-sorcery): pure,
type-driven event-sourcing primitives with in-memory and SQLite persistence.

The public API is currently under construction. Its architecture is recorded in
[`adrs/01-haskell-native-event-sourcing-architecture.md`](adrs/01-haskell-native-event-sourcing-architecture.md).

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

Direnv loads the flake automatically. The development shell provides GHC 9.14.1,
Stack, Cabal, Fourmolu, HLint, and SQLite. Stack uses the Nix-provided compiler
rather than installing its own. `nix flake check` builds the library and runs
every formatting, lint, and test gate.
