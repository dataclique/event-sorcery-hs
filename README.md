# event-sorcery-hs

Event sourcing with type-level magic, now in Haskell

The Haskell counterpart to
[`event-sorcery`](https://github.com/dataclique/event-sorcery): pure,
type-driven event-sourcing primitives with in-memory and SQLite persistence.

The public API is currently under construction. Its architecture is recorded in
[`adrs/01-haskell-native-event-sourcing-architecture.md`](adrs/01-haskell-native-event-sourcing-architecture.md).

## Development

The project is managed entirely through Nix:

```console
nix develop
nix flake check
```

The development shell provides GHC, Cabal, Haskell Language Server, Fourmolu,
HLint, and SQLite. `nix flake check` builds the library and runs every formatting,
lint, and test gate.
