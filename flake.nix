{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };
  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        lib = nixpkgs.lib;
        craneLib = crane.mkLib pkgs;
        src = lib.cleanSourceWith {
          src = craneLib.path ./.;
        };

        commonArgs = { inherit src; };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        bin = craneLib.buildPackage (commonArgs // { inherit cargoArtifacts; });

        # Cross build for Windows (x86_64-pc-windows-gnu): nix build .#windows
        pkgsWindows = import nixpkgs {
          localSystem = system;
          crossSystem = {
            config = "x86_64-w64-mingw32";
            libc = "msvcrt";
          };
        };
        windows = (crane.mkLib pkgsWindows).buildPackage (commonArgs // { strictDeps = true; });
      in
      with pkgs;
      {
        packages = {
          default = bin;
          inherit windows;
        };

        devShells.default = mkShell {
          buildInputs = [
            rust-bin.stable.latest.default
            rust-analyzer
          ];
        };

        formatter = nixpkgs-fmt;
      }
    );
}
