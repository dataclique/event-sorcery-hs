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
        haskellPackages = pkgs.haskellPackages;

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

        devShells.default = haskellPackages.shellFor {
          packages = _: [ package ];
          nativeBuildInputs = [
            haskellPackages.cabal-install
            haskellPackages.fourmolu
            haskellPackages.haskell-language-server
            haskellPackages.hlint
            haskellPackages.cabal-fmt
            pkgs.nixfmt
            pkgs.sqlite
          ];
          inherit (hooks) shellHook;
        };

        formatter = pkgs.nixfmt;
        packages.default = package;
      }
    );
}
