{
  description = "Type-driven event sourcing in Haskell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    git-hooks.url = "github:cachix/git-hooks.nix";
    git-hooks.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      git-hooks,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        haskellPackages = pkgs.haskell.packages.ghc914.override {
          overrides = _self: super: {
            protolude = pkgs.haskell.lib.doJailbreak super.protolude;
          };
        };

        package = haskellPackages.callCabal2nix "event-sorcery" self { };

        hooks = git-hooks.lib.${system}.run {
          src = self;
          hooks = {
            cabal-fmt.enable = true;
            fourmolu.enable = true;
            hlint.enable = true;
            nixfmt.enable = true;
            trim-trailing-whitespace.enable = true;
          };
        };
      in
      {
        checks = {
          inherit package;
          formatting = hooks;
        };

        devShells.default = pkgs.mkShell {
          packages = [
            haskellPackages.ghc
            pkgs.cabal-install
            pkgs.fourmolu
            pkgs.hlint
            pkgs.haskellPackages.cabal-fmt
            pkgs.stack
            pkgs.nixfmt
            pkgs.sqlite
          ];
          shellHook = hooks.shellHook;
        };

        formatter = pkgs.nixfmt;
        packages.default = package;
      }
    );
}
